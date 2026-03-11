from __future__ import annotations

import pandas as pd

from .abc_core import CLASS_PRIORITY


def build_top_changes(period_df: pd.DataFrame) -> pd.DataFrame:
    if period_df.empty:
        return pd.DataFrame(
            columns=[
                "owner_scope",
                "period_type",
                "sku",
                "prev_period",
                "curr_period",
                "prev_abc_class",
                "curr_abc_class",
                "prev_xyz_class",
                "curr_xyz_class",
                "prev_abc_xyz_class",
                "curr_abc_xyz_class",
                "class_change",
                "abc_xyz_change",
                "prev_rank",
                "curr_rank",
                "rank_delta",
                "movement_direction",
            ]
        )

    rows: list[pd.DataFrame] = []
    ordering = (
        period_df[["owner_scope", "period_type", "period_label", "period_end_date"]]
        .drop_duplicates()
        .sort_values(["owner_scope", "period_type", "period_end_date", "period_label"])
    )
    for (owner_scope, period_type), seq in ordering.groupby(["owner_scope", "period_type"], dropna=False):
        labels = seq["period_label"].tolist()
        for prev_label, curr_label in zip(labels[:-1], labels[1:]):
            prev_df = (
                period_df.loc[
                    period_df["owner_scope"].eq(owner_scope)
                    & period_df["period_type"].eq(period_type)
                    & period_df["period_label"].eq(prev_label),
                    ["sku", "abc_class", "xyz_class", "abc_xyz_class", "rank_in_period"],
                ]
                .rename(
                    columns={
                        "abc_class": "prev_abc_class",
                        "xyz_class": "prev_xyz_class",
                        "abc_xyz_class": "prev_abc_xyz_class",
                        "rank_in_period": "prev_rank",
                    }
                )
                .copy()
            )
            curr_df = (
                period_df.loc[
                    period_df["owner_scope"].eq(owner_scope)
                    & period_df["period_type"].eq(period_type)
                    & period_df["period_label"].eq(curr_label),
                    ["sku", "abc_class", "xyz_class", "abc_xyz_class", "rank_in_period"],
                ]
                .rename(
                    columns={
                        "abc_class": "curr_abc_class",
                        "xyz_class": "curr_xyz_class",
                        "abc_xyz_class": "curr_abc_xyz_class",
                        "rank_in_period": "curr_rank",
                    }
                )
                .copy()
            )
            merged = prev_df.merge(curr_df, on="sku", how="inner")
            if merged.empty:
                continue
            merged["owner_scope"] = owner_scope
            merged["period_type"] = period_type
            merged["prev_period"] = prev_label
            merged["curr_period"] = curr_label
            merged["class_change"] = merged["prev_abc_class"].astype(str) + "->" + merged["curr_abc_class"].astype(str)
            merged["abc_xyz_change"] = merged["prev_abc_xyz_class"].astype(str) + "->" + merged["curr_abc_xyz_class"].astype(str)
            merged["rank_delta"] = pd.to_numeric(merged["prev_rank"], errors="coerce") - pd.to_numeric(merged["curr_rank"], errors="coerce")
            prev_score = merged["prev_abc_class"].map(CLASS_PRIORITY).fillna(0)
            curr_score = merged["curr_abc_class"].map(CLASS_PRIORITY).fillna(0)
            merged["movement_direction"] = "stable"
            merged.loc[curr_score > prev_score, "movement_direction"] = "up"
            merged.loc[curr_score < prev_score, "movement_direction"] = "down"
            rows.append(merged)

    if not rows:
        return pd.DataFrame(
            columns=[
                "owner_scope",
                "period_type",
                "sku",
                "prev_period",
                "curr_period",
                "prev_abc_class",
                "curr_abc_class",
                "prev_xyz_class",
                "curr_xyz_class",
                "prev_abc_xyz_class",
                "curr_abc_xyz_class",
                "class_change",
                "abc_xyz_change",
                "prev_rank",
                "curr_rank",
                "rank_delta",
                "movement_direction",
            ]
        )
    return (
        pd.concat(rows, ignore_index=True)
        .sort_values(["owner_scope", "period_type", "curr_period", "movement_direction", "rank_delta"], ascending=[True, True, True, True, False])
        .reset_index(drop=True)
    )


def build_layout_candidates(period_df: pd.DataFrame, top_changes: pd.DataFrame) -> pd.DataFrame:
    if period_df.empty:
        return pd.DataFrame(
            columns=[
                "owner_scope",
                "sku",
                "denominacion",
                "latest_period_type",
                "latest_period",
                "latest_abc_class",
                "xyz_class",
                "abc_xyz_class",
                "latest_pick_lines",
                "latest_pick_qty",
                "latest_n_orders",
                "latest_rank",
                "mean_weekly_pick_lines",
                "std_weekly_pick_lines",
                "cv_weekly",
                "n_weeks_observed",
                "change_vs_prev_period",
                "recommendation_tag",
            ]
        )

    period_priority = {"ytd": 3, "quarterly": 2, "annual": 1}
    rows: list[pd.DataFrame] = []
    for owner_scope, owner_periods in period_df.groupby("owner_scope", dropna=False):
        period_meta = (
            owner_periods[["period_type", "period_label", "period_end_date"]]
            .drop_duplicates()
            .assign(priority=lambda df: df["period_type"].map(period_priority).fillna(0))
            .sort_values(["priority", "period_end_date"], ascending=[False, False])
        )
        if period_meta.empty:
            continue
        latest_period = period_meta.iloc[0]
        latest_rows = owner_periods.loc[
            owner_periods["period_type"].eq(latest_period["period_type"])
            & owner_periods["period_label"].eq(latest_period["period_label"])
        ].copy()

        latest_changes = top_changes.loc[
            top_changes["owner_scope"].eq(owner_scope)
            & top_changes["curr_period"].eq(latest_period["period_label"])
            & top_changes["period_type"].eq(latest_period["period_type"])
        ][["sku", "class_change", "abc_xyz_change", "prev_abc_class", "curr_abc_class"]].copy()

        if latest_changes.empty:
            fallback_changes = (
                top_changes.loc[top_changes["owner_scope"].eq(owner_scope)]
                .sort_values(["curr_period", "period_type"])
                .drop_duplicates(subset=["sku"], keep="last")
                [["sku", "class_change", "abc_xyz_change", "prev_abc_class", "curr_abc_class"]]
                .copy()
            )
        else:
            fallback_changes = latest_changes

        latest_rows = latest_rows.merge(fallback_changes, on="sku", how="left")
        latest_rows["change_vs_prev_period"] = latest_rows["class_change"].fillna("SIN_REFERENCIA")
        latest_rows["recommendation_tag"] = latest_rows.apply(_recommendation_tag, axis=1)
        latest_rows["latest_period_type"] = latest_period["period_type"]
        latest_rows["latest_period"] = latest_period["period_label"]
        rows.append(
            latest_rows[
                [
                    "owner_scope",
                    "sku",
                    "denominacion",
                    "latest_period_type",
                    "latest_period",
                    "abc_class",
                    "xyz_class",
                    "abc_xyz_class",
                    "pick_lines",
                    "pick_qty",
                    "n_orders",
                    "rank_in_period",
                    "mean_weekly_pick_lines",
                    "std_weekly_pick_lines",
                    "cv_weekly",
                    "n_weeks_observed",
                    "change_vs_prev_period",
                    "recommendation_tag",
                ]
            ].rename(
                columns={
                    "abc_class": "latest_abc_class",
                    "pick_lines": "latest_pick_lines",
                    "pick_qty": "latest_pick_qty",
                    "n_orders": "latest_n_orders",
                    "rank_in_period": "latest_rank",
                }
            )
        )
    return pd.concat(rows, ignore_index=True).sort_values(["owner_scope", "latest_rank", "sku"]).reset_index(drop=True)


def _recommendation_tag(row: pd.Series) -> str:
    abc_xyz = str(row.get("abc_xyz_class") or "").upper()
    if abc_xyz == "AX":
        return "KEEP_FRONT_STABLE"
    if abc_xyz == "AY":
        return "KEEP_FRONT_MONITOR"
    if abc_xyz in {"AZ", "ALOW_HISTORY", "AUNKNOWN"}:
        return "KEEP_FRONT_FLEX"
    if abc_xyz == "BX":
        return "ACCESSIBLE_STABLE"
    if abc_xyz in {"BZ", "BLOW_HISTORY", "BUNKNOWN"}:
        return "MONITOR_VOLATILE"
    if abc_xyz.startswith("C") and abc_xyz.endswith("Z"):
        return "REVIEW_SPACE"
    if abc_xyz.startswith("C"):
        return "LOW_PRIORITY"
    return "MONITOR"
