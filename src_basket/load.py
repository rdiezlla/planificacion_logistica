from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.io import apply_header_standardization
from src.service_id import normalize_code

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoadStats:
    n_rows_raw: int
    n_rows_pi: int
    n_rows_valid: int
    pct_missing_owner: float
    pct_missing_pedido_externo: float
    pct_missing_ubicacion: float


def _normalize_text(value: object, fallback: str = "") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return fallback
    text = str(value).strip()
    if not text or text.upper() in {"NAN", "NONE", "NULL"}:
        return fallback
    return text


def _normalize_numeric_id(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and float(value).is_integer():
        return str(int(value))
    text = str(value).strip().upper()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def normalize_order_id(value: object) -> str:
    text = _normalize_numeric_id(value)
    if not text:
        return ""
    code_like = normalize_code(text)
    return (code_like or text).strip().upper()


def normalize_sku(value: object) -> str:
    text = _normalize_numeric_id(value)
    return text.strip().upper()


def load_picking_lines(input_path: Path) -> tuple[pd.DataFrame, LoadStats]:
    LOGGER.info("Cargando movimientos para basket: %s", input_path)
    raw = pd.read_excel(input_path)
    df = apply_header_standardization(raw, dataset="movimientos")

    for col in [
        "tipo_movimiento",
        "fecha_inicio",
        "articulo",
        "pedido",
        "pedido_externo",
        "propietario",
        "denominacion_articulo",
        "cantidad",
        "ubicacion",
    ]:
        if col not in df.columns:
            df[col] = None

    n_rows_raw = len(df)
    tipo_mov = df["tipo_movimiento"].astype(str).str.strip().str.upper()
    pi = df.loc[tipo_mov.eq("PI")].copy()

    pi["pedido_base_raw"] = pi["pedido_externo"].where(
        pi["pedido_externo"].notna() & pi["pedido_externo"].astype(str).str.strip().ne(""),
        pi["pedido"],
    )
    pi["pedido_id"] = pi["pedido_base_raw"].map(normalize_order_id)
    pi["propietario_norm"] = pi["propietario"].map(lambda v: _normalize_numeric_id(v) or "DESCONOCIDO")
    pi["sku"] = pi["articulo"].map(normalize_sku)
    pi["sku_name"] = pi["denominacion_articulo"].map(lambda v: _normalize_text(v))
    pi["sku_name"] = pi["sku_name"].where(pi["sku_name"].ne(""), pi["sku"])
    pi["cantidad"] = pd.to_numeric(pi["cantidad"], errors="coerce").fillna(0.0)
    pi["fecha_inicio"] = pd.to_datetime(pi["fecha_inicio"], errors="coerce")
    pi["ubicacion_norm"] = pi["ubicacion"].map(lambda v: _normalize_text(v))

    valid = pi.loc[pi["pedido_id"].ne("") & pi["sku"].ne("")].copy()
    valid = valid[
        [
            "pedido_id",
            "pedido_base_raw",
            "propietario_norm",
            "sku",
            "sku_name",
            "cantidad",
            "fecha_inicio",
            "ubicacion_norm",
        ]
    ].reset_index(drop=True)

    stats = LoadStats(
        n_rows_raw=n_rows_raw,
        n_rows_pi=len(pi),
        n_rows_valid=len(valid),
        pct_missing_owner=float(pi["propietario_norm"].eq("DESCONOCIDO").mean()) if len(pi) else 0.0,
        pct_missing_pedido_externo=float(pi["pedido_externo"].isna().mean()) if "pedido_externo" in pi.columns and len(pi) else 0.0,
        pct_missing_ubicacion=float(valid["ubicacion_norm"].eq("").mean()) if len(valid) else 0.0,
    )

    LOGGER.info(
        "Basket PI: raw=%d, pi=%d, valid=%d, owner_desconocido=%.2f%%, missing_ubicacion=%.2f%%",
        stats.n_rows_raw,
        stats.n_rows_pi,
        stats.n_rows_valid,
        stats.pct_missing_owner * 100.0,
        stats.pct_missing_ubicacion * 100.0,
    )
    return valid, stats
