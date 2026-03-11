from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src_basket.load import LoadStats, load_picking_lines

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class AbcLoadStats:
    basket_stats: LoadStats
    n_rows_after_date_filter: int
    n_rows_dropped_invalid_date: int
    n_unique_skus: int
    n_unique_owners: int
    min_pick_date: pd.Timestamp | None
    max_pick_date: pd.Timestamp | None


def load_abc_picking_lines(input_path: Path) -> tuple[pd.DataFrame, AbcLoadStats]:
    lines, basket_stats = load_picking_lines(input_path)
    df = lines.copy()

    if "fecha_inicio" not in df.columns:
        raise ValueError("No existe la columna `fecha_inicio` tras cargar movimientos para ABC.")

    df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce")
    invalid_date_mask = df["fecha_inicio"].isna()
    dropped_invalid_date = int(invalid_date_mask.sum())
    if dropped_invalid_date:
        LOGGER.warning("ABC picking: se excluyen %d lineas PI por fecha_inicio invalida.", dropped_invalid_date)

    df = df.loc[~invalid_date_mask].copy()
    df["pick_date"] = df["fecha_inicio"].dt.normalize()
    df["pick_year"] = df["pick_date"].dt.year.astype(int)
    df["pick_quarter_num"] = df["pick_date"].dt.quarter.astype(int)
    df["pick_quarter"] = "Q" + df["pick_quarter_num"].astype(str)
    df["denominacion"] = df["sku_name"].fillna("").astype(str).str.strip()
    df["denominacion"] = df["denominacion"].where(df["denominacion"].ne(""), df["sku"])
    df["owner_scope"] = df.get("propietario_norm", "DESCONOCIDO")
    df["owner_scope"] = df["owner_scope"].fillna("DESCONOCIDO").astype(str).str.strip()
    df["owner_scope"] = df["owner_scope"].where(df["owner_scope"].ne(""), "DESCONOCIDO")

    min_pick_date = df["pick_date"].min() if not df.empty else None
    max_pick_date = df["pick_date"].max() if not df.empty else None

    stats = AbcLoadStats(
        basket_stats=basket_stats,
        n_rows_after_date_filter=len(df),
        n_rows_dropped_invalid_date=dropped_invalid_date,
        n_unique_skus=int(df["sku"].nunique()) if not df.empty else 0,
        n_unique_owners=int(df["owner_scope"].nunique()) if not df.empty else 0,
        min_pick_date=min_pick_date,
        max_pick_date=max_pick_date,
    )
    LOGGER.info(
        "ABC picking cargado: %d lineas validas, %d SKUs, %d owners, rango=%s -> %s",
        stats.n_rows_after_date_filter,
        stats.n_unique_skus,
        stats.n_unique_owners,
        min_pick_date,
        max_pick_date,
    )
    return df, stats
