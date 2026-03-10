from __future__ import annotations

import itertools
import logging
from collections import Counter

import pandas as pd

LOGGER = logging.getLogger(__name__)


def _get_baskets(items: pd.DataFrame) -> dict[str, list[set[str]]]:
    grouped = items.groupby(["segment", "transaction_id"], dropna=False)["sku"].agg(
        lambda s: set(s.astype(str).tolist())
    )
    baskets: dict[str, list[set[str]]] = {}
    for (segment, _tx), sku_set in grouped.items():
        baskets.setdefault(str(segment), []).append(sku_set)
    return baskets


def _support(count: int, n_tx: int) -> float:
    return count / n_tx if n_tx else 0.0


def mine_rules(
    items: pd.DataFrame,
    min_support: float,
    min_confidence: float,
    min_lift: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    baskets_by_segment = _get_baskets(items)
    rules_rows: list[dict[str, object]] = []
    triple_rows: list[dict[str, object]] = []

    for segment, baskets in baskets_by_segment.items():
        n_tx = len(baskets)
        if n_tx == 0:
            continue

        item_counter: Counter[str] = Counter()
        for basket in baskets:
            item_counter.update(basket)

        freq1 = {
            item: count
            for item, count in item_counter.items()
            if _support(count, n_tx) >= min_support
        }
        if len(freq1) < 2:
            continue

        pair_counter: Counter[tuple[str, str]] = Counter()
        for basket in baskets:
            filtered = sorted(item for item in basket if item in freq1)
            pair_counter.update(itertools.combinations(filtered, 2))
        freq2 = {
            pair: count
            for pair, count in pair_counter.items()
            if _support(count, n_tx) >= min_support
        }

        triple_counter: Counter[tuple[str, str, str]] = Counter()
        pair_set = {frozenset(pair) for pair in freq2}
        for basket in baskets:
            filtered = sorted(item for item in basket if item in freq1)
            if len(filtered) < 3:
                continue
            for triple in itertools.combinations(filtered, 3):
                if all(frozenset(pair) in pair_set for pair in itertools.combinations(triple, 2)):
                    triple_counter[triple] += 1
        freq3 = {
            triple: count
            for triple, count in triple_counter.items()
            if _support(count, n_tx) >= min_support
        }

        support_map: dict[frozenset[str], float] = {
            frozenset([item]): _support(count, n_tx) for item, count in freq1.items()
        }
        support_map.update({frozenset(pair): _support(count, n_tx) for pair, count in freq2.items()})
        support_map.update({frozenset(triple): _support(count, n_tx) for triple, count in freq3.items()})

        for pair, count in freq2.items():
            pair_set_key = frozenset(pair)
            sup_pair = support_map[pair_set_key]
            for antecedent in [(pair[0],), (pair[1],)]:
                consequent = tuple(sorted(set(pair) - set(antecedent)))
                sup_ant = support_map[frozenset(antecedent)]
                sup_cons = support_map[frozenset(consequent)]
                confidence = sup_pair / max(sup_ant, 1e-12)
                lift = confidence / max(sup_cons, 1e-12)
                if confidence >= min_confidence and lift >= min_lift:
                    rules_rows.append(
                        {
                            "segment": segment,
                            "antecedent": "|".join(antecedent),
                            "consequent": "|".join(consequent),
                            "antecedent_size": 1,
                            "consequent_size": 1,
                            "support_itemset": sup_pair,
                            "confidence": confidence,
                            "lift": lift,
                            "count_itemset": count,
                            "n_transactions": n_tx,
                        }
                    )

        for triple, count in freq3.items():
            triple_key = frozenset(triple)
            sup_triple = support_map[triple_key]
            triple_rows.append(
                {
                    "segment": segment,
                    "sku_1": triple[0],
                    "sku_2": triple[1],
                    "sku_3": triple[2],
                    "count_triple": count,
                    "support_triple": sup_triple,
                    "n_transactions": n_tx,
                }
            )
            items_sorted = list(triple)
            for r in [1, 2]:
                for antecedent in itertools.combinations(items_sorted, r):
                    antecedent_key = frozenset(antecedent)
                    consequent = tuple(sorted(set(items_sorted) - set(antecedent)))
                    sup_ant = support_map.get(antecedent_key)
                    sup_cons = support_map.get(frozenset(consequent))
                    if not sup_ant or not sup_cons:
                        continue
                    confidence = sup_triple / max(sup_ant, 1e-12)
                    lift = confidence / max(sup_cons, 1e-12)
                    if confidence >= min_confidence and lift >= min_lift:
                        rules_rows.append(
                            {
                                "segment": segment,
                                "antecedent": "|".join(sorted(antecedent)),
                                "consequent": "|".join(consequent),
                                "antecedent_size": len(antecedent),
                                "consequent_size": len(consequent),
                                "support_itemset": sup_triple,
                                "confidence": confidence,
                                "lift": lift,
                                "count_itemset": count,
                                "n_transactions": n_tx,
                            }
                        )

    rules_df = pd.DataFrame(rules_rows)
    if not rules_df.empty:
        rules_df = rules_df.sort_values(
            ["lift", "confidence", "support_itemset", "segment", "antecedent"],
            ascending=[False, False, False, True, True],
        ).reset_index(drop=True)

    triples_df = pd.DataFrame(triple_rows)
    if not triples_df.empty:
        triples_df = triples_df.sort_values(
            ["count_triple", "support_triple", "segment", "sku_1"],
            ascending=[False, False, True, True],
        ).reset_index(drop=True)

    LOGGER.info(
        "Rules calculadas: %d | triples frecuentes: %d",
        len(rules_df),
        len(triples_df),
    )
    return rules_df, triples_df
