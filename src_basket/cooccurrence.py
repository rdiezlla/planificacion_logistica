from __future__ import annotations

import itertools
import logging
import math
from collections import Counter

import pandas as pd

LOGGER = logging.getLogger(__name__)


def _segment_baskets(items: pd.DataFrame) -> dict[str, list[list[str]]]:
    baskets: dict[str, list[list[str]]] = {}
    grouped = items.groupby(["segment", "transaction_id"], dropna=False)["sku"].agg(
        lambda s: sorted(set(s.astype(str).tolist()))
    )
    for (segment, _transaction_id), sku_list in grouped.items():
        baskets.setdefault(str(segment), []).append(sku_list)
    return baskets


def compute_top_pairs(items: pd.DataFrame, sku_frequency: pd.DataFrame) -> pd.DataFrame:
    if items.empty:
        return pd.DataFrame(
            columns=[
                "segment",
                "sku_a",
                "sku_b",
                "sku_name_a",
                "sku_name_b",
                "count_pair",
                "support_pair",
                "support_a",
                "support_b",
                "lift",
                "pmi",
            ]
        )

    baskets = _segment_baskets(items)
    freq_lookup = sku_frequency.set_index(["segment", "sku"])
    rows: list[dict[str, object]] = []

    for segment, segment_baskets in baskets.items():
        n_tx = len(segment_baskets)
        pair_counter: Counter[tuple[str, str]] = Counter()
        for basket in segment_baskets:
            pair_counter.update(itertools.combinations(basket, 2))

        for (sku_a, sku_b), count_pair in pair_counter.items():
            support_pair = count_pair / n_tx
            support_a = float(freq_lookup.loc[(segment, sku_a), "support"])
            support_b = float(freq_lookup.loc[(segment, sku_b), "support"])
            denom = max(support_a * support_b, 1e-12)
            lift = support_pair / denom
            pmi = math.log2(max(support_pair, 1e-12) / denom)
            rows.append(
                {
                    "segment": segment,
                    "sku_a": sku_a,
                    "sku_b": sku_b,
                    "sku_name_a": freq_lookup.loc[(segment, sku_a), "sku_name"],
                    "sku_name_b": freq_lookup.loc[(segment, sku_b), "sku_name"],
                    "count_pair": count_pair,
                    "support_pair": support_pair,
                    "support_a": support_a,
                    "support_b": support_b,
                    "lift": lift,
                    "pmi": pmi,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out.sort_values(
        ["count_pair", "lift", "support_pair", "segment", "sku_a", "sku_b"],
        ascending=[False, False, False, True, True, True],
    ).reset_index(drop=True)
    LOGGER.info("Top pairs calculados: %d filas", len(out))
    return out
