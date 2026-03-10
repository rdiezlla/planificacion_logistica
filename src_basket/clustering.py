from __future__ import annotations

import logging
import math
import warnings

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.cluster import MiniBatchKMeans
from sklearn.decomposition import TruncatedSVD

LOGGER = logging.getLogger(__name__)


def _singleton_clusters(seg_freq: pd.DataFrame, segment: str) -> pd.DataFrame:
    out = seg_freq.copy().reset_index(drop=True)
    out["segment"] = segment
    out["cluster_id"] = range(len(out))
    out["cluster_size"] = 1
    out["cluster_transaction_count"] = out["transaction_count"]
    return out


def build_sku_clusters(pair_df: pd.DataFrame, sku_frequency: pd.DataFrame) -> pd.DataFrame:
    if sku_frequency.empty:
        return pd.DataFrame(
            columns=[
                "segment",
                "cluster_id",
                "sku",
                "sku_name",
                "transaction_count",
                "support",
                "cluster_size",
                "cluster_transaction_count",
            ]
        )

    rows: list[pd.DataFrame] = []
    for segment, seg_freq in sku_frequency.groupby("segment", dropna=False):
        seg_pairs = pair_df.loc[pair_df["segment"].eq(segment)].copy()
        seg_freq = seg_freq.sort_values(["transaction_count", "sku"], ascending=[False, True]).reset_index(drop=True)
        skus = seg_freq["sku"].astype(str).tolist()
        n_skus = len(skus)
        if n_skus == 0:
            continue

        if n_skus == 1 or seg_pairs.empty:
            rows.append(_singleton_clusters(seg_freq, segment))
            continue

        index = {sku: idx for idx, sku in enumerate(skus)}
        matrix = sparse.lil_matrix((n_skus, n_skus), dtype=float)
        for _, rec in seg_freq.iterrows():
            idx = index[str(rec["sku"])]
            matrix[idx, idx] = max(float(rec["transaction_count"]), 1.0)

        for _, rec in seg_pairs.iterrows():
            a = index.get(str(rec["sku_a"]))
            b = index.get(str(rec["sku_b"]))
            if a is None or b is None:
                continue
            weight = float(rec["count_pair"]) * max(float(rec["lift"]), 1.0)
            matrix[a, b] = weight
            matrix[b, a] = weight

        matrix = matrix.tocsr()
        if matrix.nnz <= n_skus:
            rows.append(_singleton_clusters(seg_freq, segment))
            continue
        n_components = max(2, min(12, n_skus - 1))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            embedding = TruncatedSVD(n_components=n_components, random_state=42).fit_transform(matrix)
        n_clusters = max(2, min(12, int(round(math.sqrt(max(n_skus, 2) / 2)))))
        n_clusters = min(n_clusters, n_skus)
        labels = MiniBatchKMeans(
            n_clusters=n_clusters,
            random_state=42,
            n_init=10,
            batch_size=max(128, n_clusters * 8),
        ).fit_predict(embedding)

        seg_out = seg_freq.copy()
        seg_out["segment"] = segment
        seg_out["cluster_id"] = labels
        cluster_stats = (
            seg_out.groupby("cluster_id", dropna=False)
            .agg(
                cluster_size=("sku", "size"),
                cluster_transaction_count=("transaction_count", "sum"),
            )
            .reset_index()
        )
        seg_out = seg_out.merge(cluster_stats, on="cluster_id", how="left")
        rows.append(seg_out)

    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if not out.empty:
        out = out.sort_values(
            ["segment", "cluster_transaction_count", "transaction_count", "sku"],
            ascending=[True, False, False, True],
        ).reset_index(drop=True)
    LOGGER.info("Clusters SKU calculados: %d filas", len(out))
    return out
