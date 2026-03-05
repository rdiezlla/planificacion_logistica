from __future__ import annotations

import logging
import math
import unicodedata
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from .service_classification import classify_service_type
from .service_id import build_service_id, normalize_code

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CleanConfig:
    cutoff_date: pd.Timestamp


ALB_CANONICAL = {
    "ITEM": "item",
    "FECHA SERVICIO": "fecha_servicio",
    "CONCEPTO (DENOMINACIÓN EVENTO ASOCIADO)": "concepto",
    "DESCRIPCIÓN": "descripcion",
    "PALÉS IN": "pales_in",
    "CAJAS IN": "cajas_in",
    "M3 IN": "m3_in",
    "PALÉS OUT": "pales_out",
    "M3 OUT": "m3_out",
    "CAJAS OUT": "cajas_out",
    "PESO (KG)": "peso_kg",
    "Volumen": "volumen_m3_total",
    "URGENCIA": "urgencia",
    "PROVINCIA DESTINO": "provincia_destino",
}

MOV_CANONICAL = {
    "Tipo movimiento": "tipo_movimiento",
    "Fecha inicio": "fecha_inicio",
    "Fecha finalización": "fecha_fin",
    "Artículo": "articulo",
    "Cantidad": "cantidad",
    "Pedido": "pedido",
    "Pedido externo": "pedido_externo",
    "Cliente": "cliente",
}

URGENCY_MAP = {
    "": "DESCONOCIDA",
    "N": "NO",
    "NO": "NO",
    "S": "SI",
    "SI": "SI",
    "SO": "SI",
    "MUY URGENTE": "MUY_URGENTE",
    "MUY_URGENTE": "MUY_URGENTE",
}

NUMERIC_SERVICE_COLS = [
    "pales_in",
    "cajas_in",
    "m3_in",
    "pales_out",
    "cajas_out",
    "m3_out",
    "peso_kg",
    "volumen_m3_total",
]


def _normalize_text(value: object) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    s = str(value).strip().upper()
    s = "".join(
        c
        for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )
    return " ".join(s.split())


def normalize_urgency(value: object) -> str:
    t = _normalize_text(value).replace(".", "")
    if t in URGENCY_MAP:
        return URGENCY_MAP[t]
    if "MUY" in t and "URG" in t:
        return "MUY_URGENTE"
    if t in {"SI", "S", "YES"}:
        return "SI"
    if t in {"NO", "N"}:
        return "NO"
    if not t or t in {"NAN", "NONE", "NULL"}:
        return "DESCONOCIDA"
    return "DESCONOCIDA"


def _coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = np.nan
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _safe_group_median(df: pd.DataFrame, col: str) -> pd.Series:
    med = (
        df.groupby(["tipo_servicio_final", "urgencia_norm", "mes"], dropna=False)[col]
        .transform("median")
        .astype(float)
    )
    global_med = float(df[col].median()) if df[col].notna().any() else 0.0
    return med.fillna(global_med)


def _business_zero_fill(df: pd.DataFrame, col: str) -> pd.Series:
    if col.endswith("_out"):
        return np.where(df["tipo_servicio_final"].eq("recogida"), 0.0, np.nan)
    if col.endswith("_in"):
        return np.where(df["tipo_servicio_final"].eq("entrega"), 0.0, np.nan)
    return np.full(len(df), np.nan)


def _impute_services(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["mes"] = out["fecha_servicio"].dt.month.fillna(0).astype(int)

    for c in NUMERIC_SERVICE_COLS:
        out[f"has_{c}"] = out[c].notna().astype(int)
        out[f"{c}_imputed"] = 0
        is_missing = out[c].isna()
        if not is_missing.any():
            continue

        business_zero = _business_zero_fill(out, c)
        can_zero = is_missing & ~pd.isna(business_zero)
        out.loc[can_zero, c] = business_zero[can_zero]
        out.loc[can_zero, f"{c}_imputed"] = 1

        still_missing = out[c].isna()
        if still_missing.any():
            med = _safe_group_median(out, c)
            out.loc[still_missing, c] = med[still_missing]
            out.loc[still_missing, f"{c}_imputed"] = 1

    return out.drop(columns=["mes"])


def _compute_peso_facturable(row: pd.Series) -> float:
    base = max(float(row["peso_kg"]), float(row["volumen_m3_total"]) * 270.0)
    return math.ceil(base * 10.0) / 10.0


def _split_peso_facturable(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["peso_facturable_total"] = out.apply(_compute_peso_facturable, axis=1)
    out["peso_facturable_out"] = 0.0
    out["peso_facturable_in"] = 0.0
    out["split_rule"] = ""

    entrega_mask = out["tipo_servicio_final"].eq("entrega")
    recogida_mask = out["tipo_servicio_final"].eq("recogida")
    mixto_mask = out["tipo_servicio_final"].eq("mixto")

    out.loc[entrega_mask, "peso_facturable_out"] = out.loc[entrega_mask, "peso_facturable_total"]
    out.loc[entrega_mask, "split_rule"] = "entrega_full"

    out.loc[recogida_mask, "peso_facturable_in"] = out.loc[recogida_mask, "peso_facturable_total"]
    out.loc[recogida_mask, "split_rule"] = "recogida_full"

    mixto_df = out.loc[mixto_mask, ["m3_out", "m3_in", "peso_facturable_total"]].copy()
    denom = mixto_df["m3_out"] + mixto_df["m3_in"]
    prop_out = np.where(denom > 0, mixto_df["m3_out"] / denom, 0.5)
    out.loc[mixto_mask, "peso_facturable_out"] = (
        out.loc[mixto_mask, "peso_facturable_total"] * prop_out
    )
    out.loc[mixto_mask, "peso_facturable_in"] = (
        out.loc[mixto_mask, "peso_facturable_total"] - out.loc[mixto_mask, "peso_facturable_out"]
    )
    out.loc[mixto_mask & (denom > 0), "split_rule"] = "mixto_m3_ratio"
    out.loc[mixto_mask & (denom <= 0), "split_rule"] = "mixto_50_50"

    unk_mask = out["split_rule"].eq("")
    out.loc[unk_mask, "peso_facturable_out"] = out.loc[unk_mask, "peso_facturable_total"] * 0.5
    out.loc[unk_mask, "peso_facturable_in"] = out.loc[unk_mask, "peso_facturable_total"] * 0.5
    out.loc[unk_mask, "split_rule"] = "fallback_50_50"
    return out


def clean_albaranes(raw: pd.DataFrame, cutoff_date: date | pd.Timestamp | None = None) -> pd.DataFrame:
    cutoff = (
        pd.Timestamp(cutoff_date).normalize()
        if cutoff_date is not None
        else pd.Timestamp.today().normalize()
    )
    df = raw.rename(columns=ALB_CANONICAL).copy()

    required = ["item", "fecha_servicio"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Albaranes sin columnas obligatorias: {missing}")

    df["fecha_servicio"] = pd.to_datetime(df["fecha_servicio"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["fecha_servicio"]).copy()

    for c in ["concepto", "descripcion", "provincia_destino", "urgencia", "item"]:
        if c not in df.columns:
            df[c] = None

    df["codigo_norm"] = df["item"].map(normalize_code)
    df["service_id"] = build_service_id(df["codigo_norm"], df["fecha_servicio"])

    df["urgencia_norm"] = df["urgencia"].map(normalize_urgency)

    df = _coerce_numeric(df, NUMERIC_SERVICE_COLS)

    type_rule = df.apply(
        lambda r: classify_service_type(
            r.get("codigo_norm"), r.get("concepto"), r.get("m3_in"), r.get("m3_out")
        ),
        axis=1,
        result_type="expand",
    )
    df["tipo_servicio_final"] = type_rule[0]
    df["tipo_servicio_regla"] = type_rule[1]

    df = _impute_services(df)
    df = _split_peso_facturable(df)

    df["is_historical"] = (df["fecha_servicio"] <= cutoff).astype(int)
    LOGGER.info(
        "Albaranes limpios: %d filas (historico=%d, futuro_planificado=%d)",
        len(df),
        int(df["is_historical"].sum()),
        int((1 - df["is_historical"]).sum()),
    )
    return df


def clean_movimientos(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.rename(columns=MOV_CANONICAL).copy()
    required = ["fecha_inicio", "pedido_externo", "cantidad", "articulo", "tipo_movimiento"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Movimientos sin columnas obligatorias: {missing}")

    df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce")
    df = df.dropna(subset=["fecha_inicio"]).copy()
    df["fecha_inicio_dia"] = df["fecha_inicio"].dt.normalize()

    df["pedido_externo_norm"] = df["pedido_externo"].map(normalize_code)
    df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce")
    df["cantidad"] = df["cantidad"].fillna(0.0)

    df["is_internal_or_unattributable"] = df["pedido_externo_norm"].isna().astype(int)
    return df
