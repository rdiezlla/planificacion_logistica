from __future__ import annotations

import numpy as np
import pandas as pd


def _normalize_service_type(value: object) -> str:
    txt = "" if value is None else str(value).strip().lower()
    return txt if txt else "desconocida"


def _is_delivery_component(value: object) -> bool:
    t = _normalize_service_type(value)
    return t.startswith("entrega") or t == "mixto"


def _is_pickup_component(value: object) -> bool:
    t = _normalize_service_type(value)
    return t.startswith("recogida") or t == "mixto"


def _build_observed_daily(
    service_daily: pd.DataFrame,
    workload_daily: pd.DataFrame,
    cutoff: pd.Timestamp,
) -> pd.DataFrame:
    svc = service_daily.copy()
    svc["date"] = pd.to_datetime(svc["date"], errors="coerce").dt.normalize()
    svc = svc[svc["date"].notna() & svc["date"].le(cutoff)].copy()
    if "is_historical" in svc.columns:
        svc = svc[svc["is_historical"].fillna(0).astype(int).eq(1)].copy()

    salidas = (
        svc[svc["tipo_servicio"].map(_is_delivery_component)]
        .groupby("date", dropna=False)["conteo_servicios"]
        .sum()
        .rename("salidas_real")
        .reset_index()
    )
    recogidas = (
        svc[svc["tipo_servicio"].map(_is_pickup_component)]
        .groupby("date", dropna=False)["conteo_servicios"]
        .sum()
        .rename("recogidas_real")
        .reset_index()
    )

    wl = workload_daily.copy()
    wl["date"] = pd.to_datetime(wl["date"], errors="coerce").dt.normalize()
    wl = wl[wl["date"].notna() & wl["date"].le(cutoff)].copy()
    if "is_historical" in wl.columns:
        wl = wl[wl["is_historical"].fillna(0).astype(int).eq(1)].copy()
    real_pick_col = (
        "picking_movs_atribuibles_entrega"
        if "picking_movs_atribuibles_entrega" in wl.columns
        else "picking_movs_atribuibles"
    )
    pick = (
        wl.groupby("date", dropna=False)[real_pick_col]
        .sum()
        .rename("pick_lines_real")
        .reset_index()
    )
    return salidas.merge(recogidas, on="date", how="outer").merge(pick, on="date", how="outer")


def _build_forecast_daily(
    forecast_daily_business: pd.DataFrame,
    year_target: int,
) -> pd.DataFrame:
    model = forecast_daily_business.copy()
    model["date"] = pd.to_datetime(model["fecha"], errors="coerce").dt.normalize()
    model = model[model["date"].notna()].copy()
    model = model[model["date"].dt.year.eq(year_target)].copy()
    col_map = {
        "eventos_entrega_p50": "salidas_forecast",
        "eventos_recogida_p50": "recogidas_forecast",
        "picking_movs_esperados_p50": "pick_lines_forecast",
    }
    for src in col_map:
        if src not in model.columns:
            model[src] = 0.0
    out = model[["date"] + list(col_map.keys())].rename(columns=col_map)
    for col in col_map.values():
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return out


def _build_comparison_2024_daily(observed_daily: pd.DataFrame, comparison_year: int) -> pd.DataFrame:
    base = observed_daily.copy()
    base = base[base["date"].dt.year.eq(comparison_year)].copy()
    if base.empty:
        return pd.DataFrame(columns=["week_iso", "weekday", "salidas_2024", "recogidas_2024", "pick_lines_2024"])

    iso = base["date"].dt.isocalendar()
    base["week_iso"] = iso.week.astype(int)
    base["weekday"] = base["date"].dt.dayofweek.astype(int)
    comp = (
        base.groupby(["week_iso", "weekday"], dropna=False)
        .agg(
            salidas_2024=("salidas_real", "sum"),
            recogidas_2024=("recogidas_real", "sum"),
            pick_lines_2024=("pick_lines_real", "sum"),
        )
        .reset_index()
    )
    return comp


def build_supervisor_dashboard_daily(
    *,
    forecast_daily_business: pd.DataFrame,
    service_daily: pd.DataFrame,
    workload_daily: pd.DataFrame,
    canonical_services: pd.DataFrame,
    cutoff: pd.Timestamp,
    cutover_date: pd.Timestamp,
    year_target: int = 2026,
    comparison_year: int = 2024,
) -> pd.DataFrame:
    _ = canonical_services, cutover_date
    cutoff_norm = pd.Timestamp(cutoff).normalize()
    forecast_snapshot_date = pd.Timestamp.today().normalize().strftime("%Y-%m-%d")

    observed = _build_observed_daily(service_daily=service_daily, workload_daily=workload_daily, cutoff=cutoff_norm)
    observed["date"] = pd.to_datetime(observed["date"], errors="coerce").dt.normalize()
    observed_target = observed[observed["date"].dt.year.eq(year_target)].copy()
    observed_target = observed_target.rename(
        columns={
            "salidas_real": "salidas_real_2026",
            "recogidas_real": "recogidas_real_2026",
            "pick_lines_real": "pick_lines_real_2026",
        }
    )

    comparison_2024 = _build_comparison_2024_daily(observed_daily=observed, comparison_year=comparison_year)
    forecast = _build_forecast_daily(forecast_daily_business=forecast_daily_business, year_target=year_target)

    dates = pd.concat(
        [
            forecast[["date"]] if not forecast.empty else pd.DataFrame(columns=["date"]),
            observed_target[["date"]] if not observed_target.empty else pd.DataFrame(columns=["date"]),
        ],
        ignore_index=True,
    ).drop_duplicates()
    if dates.empty:
        dates = pd.DataFrame(columns=["date"])
    out = dates.merge(forecast, on="date", how="left").merge(observed_target, on="date", how="left")

    iso = out["date"].dt.isocalendar()
    out["week_iso"] = iso.week.astype("Int64")
    out["year"] = iso.year.astype("Int64")
    out["weekday"] = out["date"].dt.dayofweek.astype("Int64")
    out["week_start_date"] = out["date"] - pd.to_timedelta(out["date"].dt.dayofweek, unit="D")
    out["week_end_date"] = out["week_start_date"] + pd.Timedelta(days=6)

    out = out.merge(comparison_2024, on=["week_iso", "weekday"], how="left")

    for c in [
        "salidas_forecast",
        "salidas_2024",
        "salidas_real_2026",
        "recogidas_forecast",
        "recogidas_2024",
        "recogidas_real_2026",
        "pick_lines_forecast",
        "pick_lines_2024",
        "pick_lines_real_2026",
    ]:
        out[c] = pd.to_numeric(out.get(c), errors="coerce").fillna(0.0)

    out["fecha"] = out["date"].dt.strftime("%Y-%m-%d")
    out["cutoff_date"] = cutoff_norm.strftime("%Y-%m-%d")
    out["forecast_snapshot_date"] = forecast_snapshot_date
    out["year_target"] = int(year_target)
    out["comparison_year"] = int(comparison_year)
    out = out.sort_values("date").reset_index(drop=True)
    return out[
        [
            "fecha",
            "year",
            "week_iso",
            "week_start_date",
            "week_end_date",
            "weekday",
            "salidas_forecast",
            "salidas_2024",
            "salidas_real_2026",
            "recogidas_forecast",
            "recogidas_2024",
            "recogidas_real_2026",
            "pick_lines_forecast",
            "pick_lines_2024",
            "pick_lines_real_2026",
            "cutoff_date",
            "forecast_snapshot_date",
            "year_target",
            "comparison_year",
        ]
    ]


def build_supervisor_dashboard_weekly(supervisor_daily: pd.DataFrame) -> pd.DataFrame:
    if supervisor_daily.empty:
        return pd.DataFrame(
            columns=[
                "year",
                "week_iso",
                "week_start_date",
                "week_end_date",
                "salidas_forecast",
                "salidas_2024",
                "salidas_real_2026",
                "recogidas_forecast",
                "recogidas_2024",
                "recogidas_real_2026",
                "pick_lines_forecast",
                "pick_lines_2024",
                "pick_lines_real_2026",
                "cutoff_date",
                "forecast_snapshot_date",
                "year_target",
                "comparison_year",
            ]
        )

    d = supervisor_daily.copy()
    d["week_start_date"] = pd.to_datetime(d["week_start_date"], errors="coerce").dt.normalize()
    d = d[d["week_start_date"].notna()].copy()
    d["week_end_date"] = d["week_start_date"] + pd.Timedelta(days=6)
    iso = d["week_start_date"].dt.isocalendar()
    d["week_iso"] = iso.week.astype("Int64")
    d["year"] = iso.year.astype("Int64")

    w = (
        d.groupby(["year", "week_iso", "week_start_date", "week_end_date"], dropna=False)
        .agg(
            salidas_forecast=("salidas_forecast", "sum"),
            salidas_2024=("salidas_2024", "sum"),
            salidas_real_2026=("salidas_real_2026", "sum"),
            recogidas_forecast=("recogidas_forecast", "sum"),
            recogidas_2024=("recogidas_2024", "sum"),
            recogidas_real_2026=("recogidas_real_2026", "sum"),
            pick_lines_forecast=("pick_lines_forecast", "sum"),
            pick_lines_2024=("pick_lines_2024", "sum"),
            pick_lines_real_2026=("pick_lines_real_2026", "sum"),
            cutoff_date=("cutoff_date", "max"),
            forecast_snapshot_date=("forecast_snapshot_date", "max"),
            year_target=("year_target", "max"),
            comparison_year=("comparison_year", "max"),
        )
        .reset_index()
    )

    w = w.sort_values("week_start_date").reset_index(drop=True)
    return w[
        [
            "year",
            "week_iso",
            "week_start_date",
            "week_end_date",
            "salidas_forecast",
            "salidas_2024",
            "salidas_real_2026",
            "recogidas_forecast",
            "recogidas_2024",
            "recogidas_real_2026",
            "pick_lines_forecast",
            "pick_lines_2024",
            "pick_lines_real_2026",
            "cutoff_date",
            "forecast_snapshot_date",
            "year_target",
            "comparison_year",
        ]
    ]
