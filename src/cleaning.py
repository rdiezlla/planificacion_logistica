from __future__ import annotations

import logging
import math
import unicodedata
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from .service_classification import classify_service_type
from .service_id import build_service_id, extract_code_from_text, normalize_code

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CleanConfig:
    cutoff_date: pd.Timestamp


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
    "volumen",
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


def _is_non_empty_text(value: object) -> bool:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return False
    text = str(value).strip().upper()
    return text not in {"", "NAN", "NONE", "NULL"}


def _parse_datetime_with_flag(
    series: pd.Series,
    *,
    dayfirst: bool = False,
    normalize: bool = False,
) -> tuple[pd.Series, pd.Series]:
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=dayfirst)
    failed = (
        parsed.isna()
        & series.notna()
        & series.astype(str).str.strip().ne("")
        & ~series.astype(str).str.strip().str.upper().isin(["NAN", "NONE", "NULL"])
    )
    if normalize:
        parsed = parsed.dt.normalize()
    return parsed, failed.astype(int)


def _choose_first_code_from_text_columns(df: pd.DataFrame, text_cols: list[str]) -> pd.Series:
    result = pd.Series(pd.NA, index=df.index, dtype="object")
    for col in text_cols:
        extracted = df[col].map(extract_code_from_text)
        result = result.where(result.notna(), extracted)
    return result


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
    base = max(float(row["peso_kg"]), float(row["volumen"]) * 270.0)
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
    df = raw.copy()

    required = ["fecha_servicio", "descripcion"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Albaranes sin columnas obligatorias: {missing}")

    for c in ["item", "concepto_evento", "descripcion", "provincia_destino", "urgencia", "festivo"]:
        if c not in df.columns:
            df[c] = None

    df["fecha_servicio"], failed_fecha_servicio = _parse_datetime_with_flag(
        df["fecha_servicio"], dayfirst=True, normalize=True
    )
    df["fecha_servicio_date_parse_failed"] = failed_fecha_servicio

    failed_count = int(df["fecha_servicio_date_parse_failed"].sum())
    if failed_count > 0:
        LOGGER.warning("fecha_servicio parse failed en %d filas de albaranes", failed_count)

    df["codigo_raw_alb"] = df["descripcion"].map(extract_code_from_text)
    df["codigo_norm_alb"] = df["codigo_raw_alb"].map(normalize_code)
    df["codigo_norm"] = df["codigo_norm_alb"]
    df["service_id"] = build_service_id(df["codigo_norm_alb"], df["fecha_servicio"])

    df["urgencia_norm"] = df["urgencia"].map(normalize_urgency)
    df["concepto"] = df["concepto_evento"]

    df = _coerce_numeric(df, NUMERIC_SERVICE_COLS)

    type_rule = df.apply(
        lambda r: classify_service_type(
            r.get("codigo_norm_alb"), r.get("concepto_evento"), r.get("m3_in"), r.get("m3_out")
        ),
        axis=1,
        result_type="expand",
    )
    df["tipo_servicio_final"] = type_rule[0]
    df["tipo_servicio_regla"] = type_rule[1]

    df = _impute_services(df)
    df["volumen_m3_total"] = df["volumen"]
    df = _split_peso_facturable(df)

    df["is_historical"] = (
        df["fecha_servicio"].notna() & (df["fecha_servicio"] <= cutoff)
    ).astype(int)

    LOGGER.info(
        "Albaranes limpios: %d filas (historico=%d, futuro_planificado=%d, fecha_parse_failed=%d)",
        len(df),
        int(df["is_historical"].sum()),
        int((1 - df["is_historical"]).sum()),
        failed_count,
    )
    return df


def clean_movimientos(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    required = [
        "tipo_movimiento",
        "fecha_inicio",
        "fecha_finalizacion",
        "articulo",
        "cantidad",
        "pedido",
        "pedido_externo",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Movimientos sin columnas obligatorias: {missing}")

    df["fecha_inicio"], failed_fecha_inicio = _parse_datetime_with_flag(df["fecha_inicio"], dayfirst=False)
    df["fecha_inicio_date_parse_failed"] = failed_fecha_inicio
    df["fecha_inicio_dia"] = df["fecha_inicio"].dt.normalize()

    df["fecha_finalizacion"], failed_fecha_fin = _parse_datetime_with_flag(
        df["fecha_finalizacion"], dayfirst=True
    )
    df["fecha_finalizacion_date_parse_failed"] = failed_fecha_fin

    fail_inicio_count = int(df["fecha_inicio_date_parse_failed"].sum())
    fail_fin_count = int(df["fecha_finalizacion_date_parse_failed"].sum())
    if fail_inicio_count > 0:
        LOGGER.warning("fecha_inicio parse failed en %d filas de movimientos", fail_inicio_count)
    if fail_fin_count > 0:
        LOGGER.warning("fecha_finalizacion parse failed en %d filas de movimientos", fail_fin_count)

    df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0.0)

    text_cols = [
        c
        for c in df.columns
        if pd.api.types.is_object_dtype(df[c]) or pd.api.types.is_string_dtype(df[c])
    ]
    regex_any = _choose_first_code_from_text_columns(df, text_cols)

    codigo_raw_mov = pd.Series(pd.NA, index=df.index, dtype="object")
    codigo_source = pd.Series(pd.NA, index=df.index, dtype="object")

    has_pedido_externo = df["pedido_externo"].map(_is_non_empty_text)
    code_pedido_externo = df["pedido_externo"].map(extract_code_from_text)
    use_pedido_externo = has_pedido_externo & code_pedido_externo.notna()
    codigo_raw_mov = codigo_raw_mov.where(~use_pedido_externo, code_pedido_externo)
    codigo_source = codigo_source.where(~use_pedido_externo, "pedido_externo")

    has_pedido = df["pedido"].map(_is_non_empty_text)
    code_pedido = df["pedido"].map(extract_code_from_text)
    use_pedido = codigo_raw_mov.isna() & has_pedido & code_pedido.notna()
    codigo_raw_mov = codigo_raw_mov.where(~use_pedido, code_pedido)
    codigo_source = codigo_source.where(~use_pedido, "pedido")

    use_regex = codigo_raw_mov.isna() & regex_any.notna()
    codigo_raw_mov = codigo_raw_mov.where(~use_regex, regex_any)
    codigo_source = codigo_source.where(~use_regex, "regex_otro")

    df["codigo_raw_mov"] = codigo_raw_mov.where(codigo_raw_mov.notna(), None)
    df["codigo_norm_mov"] = df["codigo_raw_mov"].map(normalize_code)
    df["codigo_source"] = codigo_source.where(codigo_source.notna(), None)

    df["codigo_raw_mov_regex_otro"] = regex_any.where(regex_any.notna(), None)
    df["codigo_norm_mov_regex_otro"] = df["codigo_raw_mov_regex_otro"].map(normalize_code)

    # Backward compatibility with existing downstream modules.
    df["pedido_externo_norm"] = df["codigo_norm_mov"]
    df["fecha_fin"] = df["fecha_finalizacion"]

    df["is_internal_or_unattributable"] = df["codigo_norm_mov"].isna().astype(int)
    return df
