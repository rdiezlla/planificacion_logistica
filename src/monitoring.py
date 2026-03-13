from __future__ import annotations

import numpy as np
import pandas as pd


def build_service_type_audit(service_level: pd.DataFrame) -> pd.DataFrame:
    if service_level.empty:
        return pd.DataFrame()
    df = service_level.copy()
    df["tipo_servicio_final"] = df["tipo_servicio_final"].fillna("desconocida").astype(str)
    all_counts = (
        df.groupby(["tipo_servicio_final"], dropna=False)["service_id"]
        .nunique()
        .reset_index(name="n_services")
        .assign(scope="all")
    )
    hist_counts = (
        df.loc[df["is_historical"].eq(1)]
        .groupby(["tipo_servicio_final"], dropna=False)["service_id"]
        .nunique()
        .reset_index(name="n_services")
        .assign(scope="historical")
    )
    counts = pd.concat([all_counts, hist_counts], ignore_index=True)
    counts["share_services"] = counts["n_services"] / counts.groupby("scope", dropna=False)["n_services"].transform("sum")
    counts["is_ambiguous"] = counts["tipo_servicio_final"].isin(["mixto", "desconocida"]).astype(int)
    return counts.sort_values(["scope", "n_services"], ascending=[True, False]).reset_index(drop=True)


def build_service_intensity_summary(service_daily_hist: pd.DataFrame) -> pd.DataFrame:
    if service_daily_hist.empty:
        return pd.DataFrame()
    df = service_daily_hist.copy()
    df["month"] = pd.to_datetime(df["date"], errors="coerce").dt.month
    safe_events = pd.to_numeric(df["conteo_servicios"], errors="coerce").replace(0, np.nan)
    for metric in [
        "m3_out",
        "pales_out",
        "cajas_out",
        "peso_facturable_out",
        "m3_in",
        "pales_in",
        "cajas_in",
        "peso_facturable_in",
    ]:
        if metric in df.columns:
            df[f"{metric}_por_evento"] = pd.to_numeric(df[metric], errors="coerce") / safe_events
    summary_cols = [c for c in df.columns if c.endswith("_por_evento")]
    if not summary_cols:
        return pd.DataFrame()
    summary = (
        df.groupby(["tipo_servicio", "month"], dropna=False)[summary_cols]
        .median()
        .reset_index()
    )
    return summary.sort_values(["tipo_servicio", "month"]).reset_index(drop=True)


def build_feature_policy_summary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "feature_pattern": "movtype_*",
                "status": "excluded_for_workload_training",
                "reason": "Leakage: derivado de movimientos reales del propio dia objetivo y no disponible ex ante.",
            },
            {
                "feature_pattern": "had_raw_record / was_zero_filled / calendar_status",
                "status": "excluded_for_training",
                "reason": "Flags de trazabilidad del calendario, utiles para diagnostico pero no para scoring ex ante.",
            },
        ]
    )


def build_model_health_summary(
    backtest_df: pd.DataFrame,
    model_registry_df: pd.DataFrame,
    join_kpis: pd.DataFrame,
    service_type_audit: pd.DataFrame,
    *,
    latest_cutoff_date: pd.Timestamp,
    service_hist_latest: pd.Timestamp | None,
    workload_hist_latest: pd.Timestamp | None,
) -> pd.DataFrame:
    if backtest_df.empty:
        return pd.DataFrame()

    ambiguous_share = (
        float(
            service_type_audit.loc[
                service_type_audit["scope"].eq("historical")
                & service_type_audit["is_ambiguous"].eq(1),
                "share_services",
            ].sum()
        )
        if not service_type_audit.empty
        else 0.0
    )
    join_cov = float(join_kpis["coverage"].iloc[0]) if not join_kpis.empty and "coverage" in join_kpis.columns else np.nan
    rows = []
    for (axis, freq, target), grp in backtest_df.groupby(["axis", "freq", "target"], dropna=False):
        ranked = grp.groupby("model", dropna=False)[["wape", "empirical_coverage_p80"]].mean().reset_index()
        ranked = ranked.sort_values("wape", ascending=True)
        chosen = ranked.iloc[0]["model"] if not ranked.empty else None
        coverage = (
            float(grp.loc[grp["model"].eq("model_p80"), "empirical_coverage_p80"].dropna().mean())
            if "empirical_coverage_p80" in grp.columns
            else np.nan
        )
        wape = float(grp.loc[grp["model"].eq(chosen), "wape"].dropna().mean()) if chosen is not None else np.nan
        latest_data_date = service_hist_latest if axis == "service" else workload_hist_latest
        data_gap = bool(pd.notna(latest_data_date) and latest_data_date < (latest_cutoff_date - pd.Timedelta(days=7)))
        rows.append(
            {
                "latest_cutoff_date": latest_cutoff_date,
                "axis": axis,
                "freq": freq,
                "target": target,
                "coverage_p80": coverage,
                "wape": wape,
                "chosen_model": chosen,
                "model_registered": int(
                    (
                        model_registry_df["axis"].eq(axis)
                        & model_registry_df["freq"].eq(freq)
                        & model_registry_df["target"].eq(target)
                    ).sum()
                )
                if not model_registry_df.empty
                else 0,
                "low_coverage": bool(pd.notna(coverage) and coverage < 0.75),
                "high_error": bool(pd.notna(wape) and wape > 0.35),
                "data_gap": data_gap,
                "ambiguous_service_share_high": bool(ambiguous_share > 0.10),
                "join_coverage_low": bool(pd.notna(join_cov) and join_cov < 0.20),
            }
        )
    return pd.DataFrame(rows)
