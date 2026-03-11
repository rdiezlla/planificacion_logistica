from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from streamlit_app.utils.io import file_info, read_csv_cached, read_csv_filtered_chunks


FORECAST_FILES = {
    "forecast_daily_business": "forecast_daily_business.csv",
    "forecast_weekly_business": "forecast_weekly_business.csv",
    "backtest_metrics": "backtest_metrics.csv",
    "model_registry": "model_registry.csv",
    "join_kpis": "join_kpis.csv",
    "lead_time_summary": "lead_time_summary.csv",
}

BASKET_FILES = {
    "transactions_summary_oper": "transactions_summary_oper.csv",
    "transactions_summary_order": "transactions_summary_order.csv",
    "sku_frequency_oper": "sku_frequency_oper.csv",
    "sku_frequency_order": "sku_frequency_order.csv",
    "top_pairs_oper": "top_pairs_oper.csv",
    "top_pairs_order": "top_pairs_order.csv",
    "rules_oper": "rules_oper.csv",
    "rules_order": "rules_order.csv",
    "sku_clusters_oper": "sku_clusters_oper.csv",
    "sku_clusters_order": "sku_clusters_order.csv",
    "order_owner_penalty": "order_owner_penalty.csv",
    "sku_neighbors": "sku_neighbors.csv",
}


def _safe_read(path: Path, usecols: tuple[str, ...] | None = None) -> tuple[pd.DataFrame | None, dict[str, object]]:
    info = file_info(path)
    if not info["exists"]:
        return None, info
    try:
        return read_csv_cached(str(path), usecols=usecols), info
    except Exception as exc:
        st.warning(f"No se pudo leer `{path.name}`: {exc}")
        return None, info


def load_forecast_bundle(outputs_dir: str | Path) -> dict[str, object]:
    root = Path(outputs_dir)
    bundle: dict[str, object] = {"data": {}, "files": {}}
    for key, filename in FORECAST_FILES.items():
        df, info = _safe_read(root / filename)
        bundle["data"][key] = df
        bundle["files"][key] = info
    return bundle


def load_basket_bundle(outputs_basket_dir: str | Path) -> dict[str, object]:
    root = Path(outputs_basket_dir)
    bundle: dict[str, object] = {"data": {}, "files": {}, "plots": []}
    small_usecols = {
        "top_pairs_oper": ("segment", "sku_a", "sku_b", "sku_name_a", "sku_name_b", "count_pair", "support_pair", "lift"),
        "top_pairs_order": ("segment", "sku_a", "sku_b", "sku_name_a", "sku_name_b", "count_pair", "support_pair", "lift"),
        "sku_clusters_oper": ("segment", "sku", "sku_name", "transaction_count", "cluster_id", "cluster_size", "cluster_transaction_count"),
        "sku_clusters_order": ("segment", "sku", "sku_name", "transaction_count", "cluster_id", "cluster_size", "cluster_transaction_count"),
    }
    for key, filename in BASKET_FILES.items():
        df, info = _safe_read(root / filename, usecols=small_usecols.get(key))
        bundle["data"][key] = df
        bundle["files"][key] = info
    plots_dir = root / "plots"
    if plots_dir.exists():
        bundle["plots"] = sorted(plots_dir.glob("*"))
    return bundle


def load_rules_filtered(outputs_basket_dir: str | Path, level: str, min_support: float, min_conf: float, min_lift: float, limit: int = 50000) -> tuple[pd.DataFrame | None, dict[str, object]]:
    path = Path(outputs_basket_dir) / f"rules_{level}.csv"
    info = file_info(path)
    if not info["exists"]:
        return None, info
    try:
        df = read_csv_filtered_chunks(
            str(path),
            usecols=("segment", "antecedent", "consequent", "support_itemset", "confidence", "lift", "count_itemset"),
            filters=(("support_itemset", min_support), ("confidence", min_conf), ("lift", min_lift)),
            limit=limit,
        )
        return df, info
    except Exception as exc:
        st.warning(f"No se pudo leer `{path.name}`: {exc}")
        return None, info
