from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)

CLASS_PRIORITY = {"C": 1, "B": 2, "A": 3}
STANDARD_ABC_XYZ_CLASSES = ["AX", "AY", "AZ", "BX", "BY", "BZ", "CX", "CY", "CZ"]


@dataclass(frozen=True)
class AbcThresholds:
    a_threshold: float = 0.80
    b_threshold: float = 0.95

    def __post_init__(self) -> None:
        if not (0 < self.a_threshold < self.b_threshold <= 1):
            raise ValueError("Los umbrales ABC deben cumplir 0 < A < B <= 1.")


@dataclass(frozen=True)
class XyzThresholds:
    x_threshold: float = 0.50
    y_threshold: float = 1.00

    def __post_init__(self) -> None:
        if not (0 < self.x_threshold < self.y_threshold):
            raise ValueError("Los umbrales XYZ deben cumplir 0 < X < Y.")


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


def _classify_xyz(mean_value: float, cv_value: float | None, active_weeks: int, thresholds: XyzThresholds) -> str:
    if mean_value <= 0:
        return "UNKNOWN"
    if active_weeks < 3:
        return "LOW_HISTORY"
    if cv_value is None or np.isnan(cv_value):
        return "UNKNOWN"
    if cv_value <= thresholds.x_threshold:
        return "X"
    if cv_value <= thresholds.y_threshold:
        return "Y"
    return "Z"


def _compute_weekly_stats(
    lines: pd.DataFrame,
    *,
    period_start_date: pd.Timestamp,
    period_end_date: pd.Timestamp,
    xyz_thresholds: XyzThresholds,
) -> pd.DataFrame:
    if lines.empty:
        return pd.DataFrame(
            columns=[
                "sku",
                "mean_weekly_pick_lines",
                "std_weekly_pick_lines",
                "cv_weekly",
                "n_weeks_observed",
                "xyz_class",
            ]
        )

    weekly = lines.copy()
    weekly["week_start"] = weekly["pick_date"] - pd.to_timedelta(weekly["pick_date"].dt.weekday, unit="D")
    start_week = period_start_date - pd.to_timedelta(period_start_date.weekday(), unit="D")
    end_week = period_end_date - pd.to_timedelta(period_end_date.weekday(), unit="D")
    all_weeks = pd.date_range(start=start_week, end=end_week, freq="7D")
    week_counts = (
        weekly.groupby(["sku", "week_start"], dropna=False)
        .size()
        .unstack(fill_value=0)
        .reindex(columns=all_weeks, fill_value=0)
    )
    stats = pd.DataFrame(index=week_counts.index)
    stats["mean_weekly_pick_lines"] = week_counts.mean(axis=1)
    stats["std_weekly_pick_lines"] = week_counts.std(axis=1, ddof=0)
    stats["cv_weekly"] = np.where(
        stats["mean_weekly_pick_lines"] > 0,
        stats["std_weekly_pick_lines"] / stats["mean_weekly_pick_lines"],
        np.nan,
    )
    stats["n_weeks_observed"] = (week_counts > 0).sum(axis=1).astype(int)
    stats["xyz_class"] = [
        _classify_xyz(
            float(row["mean_weekly_pick_lines"]),
            None if pd.isna(row["cv_weekly"]) else float(row["cv_weekly"]),
            int(row["n_weeks_observed"]),
            xyz_thresholds,
        )
        for _, row in stats.iterrows()
    ]
    return stats.reset_index()


def summarize_period(
    lines: pd.DataFrame,
    *,
    owner_scope: str,
    period_type: str,
    year: int,
    period_label: str,
    abc_thresholds: AbcThresholds,
    xyz_thresholds: XyzThresholds,
    quarter: str | None = None,
    period_start_date: pd.Timestamp | None = None,
    period_end_date: pd.Timestamp | None = None,
    cutoff_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    base_columns = [
        "owner_scope",
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
        "mean_weekly_pick_lines",
        "std_weekly_pick_lines",
        "cv_weekly",
        "n_weeks_observed",
        "xyz_class",
        "abc_xyz_class",
    ]
    if lines.empty:
        return pd.DataFrame(columns=base_columns)

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

    weekly_stats = _compute_weekly_stats(
        lines,
        period_start_date=period_start_date or lines["pick_date"].min(),
        period_end_date=period_end_date or lines["pick_date"].max(),
        xyz_thresholds=xyz_thresholds,
    )
    grouped = grouped.merge(weekly_stats, on="sku", how="left")

    total_pick_lines = float(grouped["pick_lines"].sum())
    grouped["share_pct"] = grouped["pick_lines"] / total_pick_lines if total_pick_lines > 0 else 0.0
    grouped["cumulative_pct"] = grouped["share_pct"].cumsum()
    grouped["abc_class"] = _classify_abc(grouped["share_pct"], grouped["cumulative_pct"], abc_thresholds)
    grouped["rank_in_period"] = np.arange(1, len(grouped) + 1, dtype=int)
    grouped["abc_xyz_class"] = grouped["abc_class"].astype(str) + grouped["xyz_class"].astype(str)
    grouped["owner_scope"] = owner_scope
    grouped["period_type"] = period_type
    grouped["year"] = int(year)
    grouped["quarter"] = quarter if quarter is not None else pd.NA
    grouped["period_label"] = period_label
    grouped["period_start_date"] = period_start_date
    grouped["period_end_date"] = period_end_date
    grouped["cutoff_date"] = cutoff_date if cutoff_date is not None else period_end_date

    return grouped[base_columns]


def determine_owner_scopes(lines: pd.DataFrame, max_owners: int = 10) -> list[str]:
    if lines.empty or "owner_scope" not in lines.columns:
        return ["GLOBAL"]
    owner_rank = (
        lines.groupby("owner_scope", dropna=False)
        .size()
        .sort_values(ascending=False)
    )
    top_owners = owner_rank.head(max_owners).index.astype(str).tolist()
    ordered = ["GLOBAL"] + [owner for owner in top_owners if owner != "GLOBAL"]
    return ordered


def build_owner_summary(lines: pd.DataFrame, owner_scopes: list[str]) -> pd.DataFrame:
    if lines.empty:
        return pd.DataFrame(
            columns=["owner_scope", "n_skus", "total_pick_lines", "pct_total_pick_lines", "top_sku", "top_sku_pick_lines"]
        )

    total_lines_global = float(len(lines))
    rows: list[dict[str, object]] = []
    for owner_scope in owner_scopes:
        subset = lines if owner_scope == "GLOBAL" else lines.loc[lines["owner_scope"].eq(owner_scope)]
        if subset.empty:
            continue
        top_sku = subset["sku"].value_counts().head(1)
        rows.append(
            {
                "owner_scope": owner_scope,
                "n_skus": int(subset["sku"].nunique()),
                "total_pick_lines": int(len(subset)),
                "pct_total_pick_lines": float(len(subset) / total_lines_global) if total_lines_global else 0.0,
                "top_sku": None if top_sku.empty else str(top_sku.index[0]),
                "top_sku_pick_lines": None if top_sku.empty else int(top_sku.iloc[0]),
            }
        )
    out = pd.DataFrame(rows)
    owner_priority = {owner: idx for idx, owner in enumerate(owner_scopes)}
    if not out.empty:
        out["owner_priority"] = out["owner_scope"].map(owner_priority)
        out = out.sort_values(["owner_priority", "total_pick_lines"], ascending=[True, False]).drop(columns=["owner_priority"])
    return out.reset_index(drop=True)


def build_annual_abc(lines: pd.DataFrame, abc_thresholds: AbcThresholds, xyz_thresholds: XyzThresholds, owner_scopes: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for owner_scope in owner_scopes:
        owner_lines = lines if owner_scope == "GLOBAL" else lines.loc[lines["owner_scope"].eq(owner_scope)].copy()
        if owner_lines.empty:
            continue
        for year in sorted(owner_lines["pick_year"].dropna().unique().tolist()):
            subset = owner_lines.loc[owner_lines["pick_year"].eq(int(year))].copy()
            frames.append(
                summarize_period(
                    subset,
                    owner_scope=owner_scope,
                    period_type="annual",
                    year=int(year),
                    period_label=str(int(year)),
                    abc_thresholds=abc_thresholds,
                    xyz_thresholds=xyz_thresholds,
                    period_start_date=pd.Timestamp(year=int(year), month=1, day=1),
                    period_end_date=subset["pick_date"].max(),
                )
            )
    return pd.concat(frames, ignore_index=True) if frames else summarize_period(pd.DataFrame(), owner_scope="GLOBAL", period_type="annual", year=0, period_label="", abc_thresholds=abc_thresholds, xyz_thresholds=xyz_thresholds)


def build_quarterly_abc(lines: pd.DataFrame, abc_thresholds: AbcThresholds, xyz_thresholds: XyzThresholds, owner_scopes: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for owner_scope in owner_scopes:
        owner_lines = lines if owner_scope == "GLOBAL" else lines.loc[lines["owner_scope"].eq(owner_scope)].copy()
        if owner_lines.empty:
            continue
        grouped = owner_lines.groupby(["pick_year", "pick_quarter_num"], dropna=False)
        for (year, quarter_num), subset in grouped:
            if pd.isna(year) or pd.isna(quarter_num):
                continue
            quarter = f"Q{int(quarter_num)}"
            period_label = f"{int(year)}-{quarter}"
            frames.append(
                summarize_period(
                    subset.copy(),
                    owner_scope=owner_scope,
                    period_type="quarterly",
                    year=int(year),
                    quarter=quarter,
                    period_label=period_label,
                    abc_thresholds=abc_thresholds,
                    xyz_thresholds=xyz_thresholds,
                    period_start_date=subset["pick_date"].min(),
                    period_end_date=subset["pick_date"].max(),
                )
            )
    return pd.concat(frames, ignore_index=True) if frames else summarize_period(pd.DataFrame(), owner_scope="GLOBAL", period_type="quarterly", year=0, period_label="", abc_thresholds=abc_thresholds, xyz_thresholds=xyz_thresholds)


def build_ytd_abc(
    lines: pd.DataFrame,
    abc_thresholds: AbcThresholds,
    xyz_thresholds: XyzThresholds,
    owner_scopes: list[str],
    reference_year: int | None = None,
) -> pd.DataFrame:
    if lines.empty:
        return summarize_period(pd.DataFrame(), owner_scope="GLOBAL", period_type="ytd", year=0, period_label="", abc_thresholds=abc_thresholds, xyz_thresholds=xyz_thresholds)

    current_year = pd.Timestamp.today().year
    frames: list[pd.DataFrame] = []
    for owner_scope in owner_scopes:
        owner_lines = lines if owner_scope == "GLOBAL" else lines.loc[lines["owner_scope"].eq(owner_scope)].copy()
        if owner_lines.empty:
            continue
        available_years = sorted(owner_lines["pick_year"].dropna().astype(int).unique().tolist())
        ytd_year = reference_year or (current_year if current_year in available_years else max(available_years))
        subset = owner_lines.loc[owner_lines["pick_year"].eq(int(ytd_year))].copy()
        if subset.empty:
            continue
        cutoff_date = subset["pick_date"].max()
        period_start = pd.Timestamp(year=int(ytd_year), month=1, day=1)
        subset = subset.loc[subset["pick_date"].between(period_start, cutoff_date)].copy()
        frames.append(
            summarize_period(
                subset,
                owner_scope=owner_scope,
                period_type="ytd",
                year=int(ytd_year),
                quarter=None,
                period_label=f"{int(ytd_year)}-YTD",
                abc_thresholds=abc_thresholds,
                xyz_thresholds=xyz_thresholds,
                period_start_date=period_start,
                period_end_date=cutoff_date,
                cutoff_date=cutoff_date,
            )
        )
    return pd.concat(frames, ignore_index=True) if frames else summarize_period(pd.DataFrame(), owner_scope="GLOBAL", period_type="ytd", year=0, period_label="", abc_thresholds=abc_thresholds, xyz_thresholds=xyz_thresholds)


def build_summary_by_period(period_df: pd.DataFrame) -> pd.DataFrame:
    if period_df.empty:
        return pd.DataFrame(
            columns=[
                "owner_scope",
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
    for keys, subset in period_df.groupby(["owner_scope", "period_type", "period_label", "year", "quarter"], dropna=False):
        owner_scope, period_type, period_label, year, quarter = keys
        total_pick_lines = float(subset["pick_lines"].sum())
        top = subset.sort_values(["rank_in_period", "pick_lines"], ascending=[True, False]).head(1)
        rows.append(
            {
                "owner_scope": owner_scope,
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
    return pd.DataFrame(rows).sort_values(["owner_scope", "period_end_date", "period_type", "period_label"]).reset_index(drop=True)


def build_abc_xyz_summary_by_period(period_df: pd.DataFrame) -> pd.DataFrame:
    if period_df.empty:
        return pd.DataFrame(columns=["owner_scope", "period_type", "period_label"])

    rows: list[dict[str, object]] = []
    observed_classes = sorted(period_df["abc_xyz_class"].dropna().astype(str).unique().tolist())
    all_classes = STANDARD_ABC_XYZ_CLASSES + [cls for cls in observed_classes if cls not in STANDARD_ABC_XYZ_CLASSES]
    for keys, subset in period_df.groupby(["owner_scope", "period_type", "period_label"], dropna=False):
        owner_scope, period_type, period_label = keys
        total_pick_lines = float(subset["pick_lines"].sum())
        row: dict[str, object] = {
            "owner_scope": owner_scope,
            "period_type": period_type,
            "period_label": period_label,
        }
        for class_name in all_classes:
            mask = subset["abc_xyz_class"].astype(str).eq(class_name)
            safe_class = class_name.replace("-", "_")
            row[f"n_skus_{safe_class}"] = int(mask.sum())
            row[f"pct_pick_lines_{safe_class}"] = float(subset.loc[mask, "pick_lines"].sum() / total_pick_lines) if total_pick_lines else 0.0
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["owner_scope", "period_type", "period_label"]).reset_index(drop=True)
