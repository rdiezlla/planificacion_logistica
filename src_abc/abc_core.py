from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)

CLASS_PRIORITY = {"C": 1, "B": 2, "A": 3}


@dataclass(frozen=True)
class AbcThresholds:
    a_threshold: float = 0.80
    b_threshold: float = 0.95

    def __post_init__(self) -> None:
        if not (0 < self.a_threshold < self.b_threshold <= 1):
            raise ValueError("Los umbrales ABC deben cumplir 0 < A < B <= 1.")


def _preferred_name(series: pd.Series) -> str:
    clean = series.fillna("").astype(str).str.strip()
    clean = clean.loc[clean.ne("")]
    if clean.empty:
        return ""
    mode = clean.mode()
    if not mode.empty:
        return str(mode.iloc[0])
    return str(clean.iloc[0])


def _classify_abc(share: pd.Series, cumulative: pd.Series, thresholds: AbcThresholds) -> pd.Series:
    prev_cumulative = (cumulative - share).fillna(0.0)
    classes = np.where(
        prev_cumulative < thresholds.a_threshold,
        "A",
        np.where(prev_cumulative < thresholds.b_threshold, "B", "C"),
    )
    if len(classes):
        classes[0] = "A"
    return pd.Series(classes, index=share.index, dtype="object")


def summarize_period(
    lines: pd.DataFrame,
    *,
    period_type: str,
    year: int,
    period_label: str,
    thresholds: AbcThresholds,
    quarter: str | None = None,
    period_start_date: pd.Timestamp | None = None,
    period_end_date: pd.Timestamp | None = None,
    cutoff_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    if lines.empty:
        return pd.DataFrame(
            columns=[
                "period_type",
                "year",
                "quarter",
                "period_label",
                "period_start_date",
                "period_end_date",
                "cutoff_date",
                "sku",
                "denominacion",
                "pick_lines",
                "pick_qty",
                "n_orders",
                "n_days_active",
                "last_pick_date",
                "share_pct",
                "cumulative_pct",
                "abc_class",
                "rank_in_period",
            ]
        )

    grouped = (
        lines.groupby("sku", dropna=False)
        .agg(
            denominacion=("denominacion", _preferred_name),
            pick_lines=("sku", "size"),
            pick_qty=("cantidad", "sum"),
            n_orders=("pedido_id", "nunique"),
            n_days_active=("pick_date", "nunique"),
            last_pick_date=("pick_date", "max"),
        )
        .reset_index()
        .sort_values(["pick_lines", "pick_qty", "n_orders", "sku"], ascending=[False, False, False, True])
        .reset_index(drop=True)
    )

    total_pick_lines = float(grouped["pick_lines"].sum())
    grouped["share_pct"] = grouped["pick_lines"] / total_pick_lines if total_pick_lines > 0 else 0.0
    grouped["cumulative_pct"] = grouped["share_pct"].cumsum()
    grouped["abc_class"] = _classify_abc(grouped["share_pct"], grouped["cumulative_pct"], thresholds)
    grouped["rank_in_period"] = np.arange(1, len(grouped) + 1, dtype=int)
    grouped["period_type"] = period_type
    grouped["year"] = int(year)
    grouped["quarter"] = quarter if quarter is not None else pd.NA
    grouped["period_label"] = period_label
    grouped["period_start_date"] = period_start_date
    grouped["period_end_date"] = period_end_date
    grouped["cutoff_date"] = cutoff_date if cutoff_date is not None else period_end_date

    return grouped[
        [
            "period_type",
            "year",
            "quarter",
            "period_label",
            "period_start_date",
            "period_end_date",
            "cutoff_date",
            "sku",
            "denominacion",
            "pick_lines",
            "pick_qty",
            "n_orders",
            "n_days_active",
            "last_pick_date",
            "share_pct",
            "cumulative_pct",
            "abc_class",
            "rank_in_period",
        ]
    ]


def build_annual_abc(lines: pd.DataFrame, thresholds: AbcThresholds) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year in sorted(lines["pick_year"].dropna().unique().tolist()):
        subset = lines.loc[lines["pick_year"].eq(int(year))].copy()
        frames.append(
            summarize_period(
                subset,
                period_type="annual",
                year=int(year),
                period_label=str(int(year)),
                thresholds=thresholds,
                period_start_date=pd.Timestamp(year=int(year), month=1, day=1),
                period_end_date=subset["pick_date"].max(),
            )
        )
    return pd.concat(frames, ignore_index=True) if frames else summarize_period(pd.DataFrame(), period_type="annual", year=0, period_label="", thresholds=thresholds)


def build_quarterly_abc(lines: pd.DataFrame, thresholds: AbcThresholds) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    grouped = lines.groupby(["pick_year", "pick_quarter_num"], dropna=False)
    for (year, quarter_num), subset in grouped:
        if pd.isna(year) or pd.isna(quarter_num):
            continue
        quarter = f"Q{int(quarter_num)}"
        period_label = f"{int(year)}-{quarter}"
        frames.append(
            summarize_period(
                subset.copy(),
                period_type="quarterly",
                year=int(year),
                quarter=quarter,
                period_label=period_label,
                thresholds=thresholds,
                period_start_date=subset["pick_date"].min(),
                period_end_date=subset["pick_date"].max(),
            )
        )
    return pd.concat(frames, ignore_index=True) if frames else summarize_period(pd.DataFrame(), period_type="quarterly", year=0, period_label="", thresholds=thresholds)


def build_ytd_abc(lines: pd.DataFrame, thresholds: AbcThresholds, reference_year: int | None = None) -> pd.DataFrame:
    if lines.empty:
        return summarize_period(pd.DataFrame(), period_type="ytd", year=0, period_label="", thresholds=thresholds)

    available_years = sorted(lines["pick_year"].dropna().astype(int).unique().tolist())
    current_year = pd.Timestamp.today().year
    ytd_year = reference_year or (current_year if current_year in available_years else max(available_years))
    subset = lines.loc[lines["pick_year"].eq(int(ytd_year))].copy()
    if subset.empty:
        return summarize_period(pd.DataFrame(), period_type="ytd", year=int(ytd_year), period_label=f"{int(ytd_year)}-YTD", thresholds=thresholds)

    cutoff_date = subset["pick_date"].max()
    period_start = pd.Timestamp(year=int(ytd_year), month=1, day=1)
    subset = subset.loc[subset["pick_date"].between(period_start, cutoff_date)].copy()
    return summarize_period(
        subset,
        period_type="ytd",
        year=int(ytd_year),
        quarter=None,
        period_label=f"{int(ytd_year)}-YTD",
        thresholds=thresholds,
        period_start_date=period_start,
        period_end_date=cutoff_date,
        cutoff_date=cutoff_date,
    )


def build_summary_by_period(period_df: pd.DataFrame) -> pd.DataFrame:
    if period_df.empty:
        return pd.DataFrame(
            columns=[
                "period_type",
                "period_label",
                "year",
                "quarter",
                "period_start_date",
                "period_end_date",
                "cutoff_date",
                "n_skus",
                "n_skus_A",
                "n_skus_B",
                "n_skus_C",
                "pct_pick_lines_A",
                "pct_pick_lines_B",
                "pct_pick_lines_C",
                "top_sku",
                "top_sku_pick_lines",
            ]
        )

    rows: list[dict[str, object]] = []
    for keys, subset in period_df.groupby(["period_type", "period_label", "year", "quarter"], dropna=False):
        period_type, period_label, year, quarter = keys
        total_pick_lines = float(subset["pick_lines"].sum())
        top = subset.sort_values(["rank_in_period", "pick_lines"], ascending=[True, False]).head(1)
        rows.append(
            {
                "period_type": period_type,
                "period_label": period_label,
                "year": year,
                "quarter": quarter,
                "period_start_date": subset["period_start_date"].max(),
                "period_end_date": subset["period_end_date"].max(),
                "cutoff_date": subset["cutoff_date"].max(),
                "n_skus": int(subset["sku"].nunique()),
                "n_skus_A": int(subset["abc_class"].eq("A").sum()),
                "n_skus_B": int(subset["abc_class"].eq("B").sum()),
                "n_skus_C": int(subset["abc_class"].eq("C").sum()),
                "pct_pick_lines_A": float(subset.loc[subset["abc_class"].eq("A"), "pick_lines"].sum() / total_pick_lines) if total_pick_lines else 0.0,
                "pct_pick_lines_B": float(subset.loc[subset["abc_class"].eq("B"), "pick_lines"].sum() / total_pick_lines) if total_pick_lines else 0.0,
                "pct_pick_lines_C": float(subset.loc[subset["abc_class"].eq("C"), "pick_lines"].sum() / total_pick_lines) if total_pick_lines else 0.0,
                "top_sku": None if top.empty else top.iloc[0]["sku"],
                "top_sku_pick_lines": None if top.empty else int(top.iloc[0]["pick_lines"]),
            }
        )
    return pd.DataFrame(rows).sort_values(["period_end_date", "period_type", "period_label"]).reset_index(drop=True)

