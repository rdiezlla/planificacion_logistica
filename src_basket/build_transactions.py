from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

LOGGER = logging.getLogger(__name__)


@dataclass
class TransactionView:
    level: str
    summary_all: pd.DataFrame
    summary_eligible: pd.DataFrame
    items_eligible: pd.DataFrame
    sku_frequency: pd.DataFrame


def _build_summary(
    lines: pd.DataFrame,
    tx_keys: list[str],
    level: str,
    max_basket_size: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    unique_items = lines.drop_duplicates(tx_keys + ["sku"]).copy()

    summary = (
        lines.groupby(tx_keys, dropna=False)
        .agg(
            n_lineas=("sku", "size"),
            cantidad_total=("cantidad", "sum"),
            fecha_inicio_min=("fecha_inicio", "min"),
            fecha_inicio_max=("fecha_inicio", "max"),
        )
        .reset_index()
    )

    n_skus = (
        unique_items.groupby(tx_keys, dropna=False)
        .agg(
            n_skus=("sku", "size"),
            sku_list=("sku", lambda s: "|".join(sorted(s.astype(str).tolist()))),
        )
        .reset_index()
    )
    summary = summary.merge(n_skus, on=tx_keys, how="left")
    summary["basket_flag"] = "eligible"
    summary.loc[summary["n_skus"] < 2, "basket_flag"] = "lt2_skus"
    summary.loc[summary["n_skus"] > max_basket_size, "basket_flag"] = "gt_max_basket_size"
    summary["is_eligible"] = summary["basket_flag"].eq("eligible").astype(int)
    summary["level"] = level

    if level == "oper":
        summary["transaction_id"] = summary["pedido_id"] + "__" + summary["propietario_norm"]
        summary["segment"] = summary["propietario_norm"]
    else:
        summary["transaction_id"] = summary["pedido_id"]
        summary["segment"] = "ALL"

    eligible_tx = summary.loc[summary["is_eligible"].eq(1), tx_keys + ["transaction_id", "segment"]].copy()
    items = unique_items.merge(eligible_tx, on=tx_keys, how="inner")
    items = items[["transaction_id", "segment", "pedido_id", "propietario_norm", "sku", "sku_name", "ubicacion_norm"]].copy()
    return summary, items


def _build_sku_frequency(items: pd.DataFrame) -> pd.DataFrame:
    if items.empty:
        return pd.DataFrame(
            columns=["segment", "sku", "sku_name", "transaction_count", "support", "n_transactions"]
        )

    tx_per_segment = items.groupby("segment")["transaction_id"].nunique().rename("n_transactions")
    freq = (
        items.groupby(["segment", "sku"], dropna=False)
        .agg(
            sku_name=("sku_name", lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0]),
            transaction_count=("transaction_id", "nunique"),
            ubicacion_mode=("ubicacion_norm", lambda s: s.mode().iat[0] if not s.mode().empty else ""),
        )
        .reset_index()
        .merge(tx_per_segment.reset_index(), on="segment", how="left")
    )
    freq["support"] = freq["transaction_count"] / freq["n_transactions"].clip(lower=1)
    return freq.sort_values(["segment", "transaction_count", "sku"], ascending=[True, False, True]).reset_index(drop=True)


def build_transaction_views(lines: pd.DataFrame, max_basket_size: int) -> dict[str, TransactionView]:
    base = lines.copy()
    oper_summary, oper_items = _build_summary(
        base,
        tx_keys=["pedido_id", "propietario_norm"],
        level="oper",
        max_basket_size=max_basket_size,
    )
    order_summary, order_items = _build_summary(
        base,
        tx_keys=["pedido_id"],
        level="order",
        max_basket_size=max_basket_size,
    )

    order_summary["propietario_norm"] = "MULTI"

    LOGGER.info(
        "Transacciones basket | oper eligible=%d/%d | order eligible=%d/%d | max_basket_size=%d",
        int(oper_summary["is_eligible"].sum()),
        len(oper_summary),
        int(order_summary["is_eligible"].sum()),
        len(order_summary),
        max_basket_size,
    )

    return {
        "oper": TransactionView(
            level="oper",
            summary_all=oper_summary,
            summary_eligible=oper_summary.loc[oper_summary["is_eligible"].eq(1)].copy(),
            items_eligible=oper_items,
            sku_frequency=_build_sku_frequency(oper_items),
        ),
        "order": TransactionView(
            level="order",
            summary_all=order_summary,
            summary_eligible=order_summary.loc[order_summary["is_eligible"].eq(1)].copy(),
            items_eligible=order_items,
            sku_frequency=_build_sku_frequency(order_items),
        ),
    }
