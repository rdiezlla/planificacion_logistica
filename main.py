from __future__ import annotations

import argparse
import logging
import math
from pathlib import Path

import numpy as np
import pandas as pd

from src.backtest import BacktestConfig, run_backtest
from src.cleaning import clean_albaranes, clean_movimientos
from src.feature_engineering import add_time_features, parse_exclude_years
from src.geo_normalization import normalize_provincia_destino
from src.io import (
    load_albaranes,
    load_holidays,
    load_movimientos,
    load_provincia_station_map,
    resolve_input_paths,
)
from src.join_assignment import assign_movements_to_services, generate_join_debug_outputs
from src.predict import collect_model_paths, predict_targets_wide
from src.report import save_backtest_plots, save_forecast_plots, save_picking_validation_plot
from src.targets import (
    build_service_level,
    build_service_targets,
    build_workload_targets,
    transform_service_forecast_to_workload_expected,
)
from src.train import train_and_save_models
from src.weather_aemet import build_weighted_weather

LOGGER = logging.getLogger("pipeline")

SERVICE_TARGETS = [
    "conteo_servicios",
    "m3_out",
    "cajas_out",
    "pales_out",
    "peso_facturable_out",
    "m3_in",
    "cajas_in",
    "pales_in",
    "peso_facturable_in",
]

WORKLOAD_TARGETS = [
    "conteo_movimientos",
    "unidades_movidas",
    "skus_distintos",
    "workload_atribuible_movs",
    "workload_no_atribuible_movs",
    "picking_movs",
    "picking_movs_atribuibles",
    "picking_movs_atribuibles_entrega",
    "picking_movs_no_atribuibles",
]


COUNT_TOKENS = ["conteo", "mov", "pales", "cajas", "eventos", "skus", "unidades", "workload"]
PHYSICAL_TOKENS = ["m3", "peso", "volumen"]


def _normalize_service_type(value: object) -> str:
    if value is None:
        return "desconocida"
    t = str(value).strip().lower()
    return t if t else "desconocida"


def _is_delivery_type(value: object) -> bool:
    return _normalize_service_type(value).startswith("entrega")


def _is_pickup_type(value: object) -> bool:
    return _normalize_service_type(value).startswith("recogida")


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "si"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline forecasting logistica (servicio + workload)")
    parser.add_argument("--horizon_days", type=int, default=60)
    parser.add_argument("--freq", type=str, default="both", choices=["daily", "weekly", "both"])
    parser.add_argument("--use_weather", type=str, default="true")
    parser.add_argument("--assignment_window_days", type=int, default=30)
    parser.add_argument("--debug_join", type=str, default="false")
    parser.add_argument("--exclude_years", type=str, default="2025")
    parser.add_argument("--cutoff_date", type=str, default=None)
    return parser.parse_args()


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _safe_cols(df: pd.DataFrame, cols: list[str]) -> list[str]:
    return [c for c in cols if c in df.columns]


def _is_count_metric(metric_name: str) -> bool:
    name = metric_name.lower()
    if any(token in name for token in PHYSICAL_TOKENS):
        return False
    return any(token in name for token in COUNT_TOKENS)


def _post_process_forecasts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    pred_cols = [c for c in out.columns if c.endswith("_p50") or c.endswith("_p80")]

    for col in pred_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")
        out[col] = out[col].fillna(0.0)
        out[col] = out[col].clip(lower=0)

    base_metrics = sorted({c[:-4] for c in pred_cols if c.endswith("_p50") or c.endswith("_p80")})
    for metric in base_metrics:
        p50_col = f"{metric}_p50"
        p80_col = f"{metric}_p80"

        if _is_count_metric(metric):
            if p50_col in out.columns:
                out[p50_col] = np.round(out[p50_col])
            if p80_col in out.columns:
                out[p80_col] = np.ceil(out[p80_col])

        if p50_col in out.columns and p80_col in out.columns:
            out[p80_col] = np.maximum(out[p80_col], out[p50_col])

    return out


def _aggregate_weekly_from_daily(
    daily_df: pd.DataFrame,
    target_cols: list[str],
    axis: str,
) -> pd.DataFrame:
    d = daily_df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.normalize()
    d["week_start"] = d["date"] - pd.to_timedelta(d["date"].dt.dayofweek, unit="D")

    group_cols = ["week_start"]
    if "tipo_servicio" in d.columns:
        group_cols.append("tipo_servicio")

    agg = {c: "sum" for c in target_cols if c in d.columns}
    if "is_historical" in d.columns:
        agg["is_historical"] = "max"

    w = d.groupby(group_cols, dropna=False).agg(agg).reset_index().rename(columns={"week_start": "date"})
    w["axis"] = axis
    if "tipo_servicio" not in w.columns:
        w["tipo_servicio"] = "ALL"
    return w


def _build_future_frame(
    cutoff: pd.Timestamp,
    horizon_days: int,
    freq: str,
    axis: str,
    tipo_servicios: list[str],
) -> pd.DataFrame:
    if freq == "daily":
        dates = pd.date_range(cutoff + pd.Timedelta(days=1), periods=horizon_days, freq="D")
    else:
        weeks = int(math.ceil(horizon_days / 7.0))
        next_monday = cutoff + pd.Timedelta(days=(7 - cutoff.dayofweek) % 7)
        if next_monday <= cutoff:
            next_monday = next_monday + pd.Timedelta(days=7)
        dates = pd.date_range(next_monday, periods=weeks, freq="7D")

    rows = []
    if axis == "service":
        for dt in dates:
            for tipo in tipo_servicios:
                rows.append({"date": dt, "axis": axis, "tipo_servicio": tipo})
    else:
        for dt in dates:
            rows.append({"date": dt, "axis": axis, "tipo_servicio": "ALL"})
    return pd.DataFrame(rows)


def run_model_block(
    hist_df: pd.DataFrame,
    all_df: pd.DataFrame,
    holidays_df: pd.DataFrame,
    weather_daily: pd.DataFrame,
    axis: str,
    freq: str,
    targets: list[str],
    cutoff: pd.Timestamp,
    horizon_days: int,
    models_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    target_cols = _safe_cols(hist_df, targets)
    if not target_cols:
        raise ValueError(f"No hay targets disponibles para axis={axis}, freq={freq}")

    hist_feat = add_time_features(hist_df, holidays_df=holidays_df, weather_daily=weather_daily)

    segment_cols = ["tipo_servicio"] if axis == "service" else []
    backtest = run_backtest(
        hist_feat,
        target_cols=target_cols,
        axis=axis,
        freq=freq,
        segment_cols=segment_cols,
        config=BacktestConfig(min_train_days=120 if freq == "daily" else 26, test_days=28 if freq == "daily" else 8, step_days=28 if freq == "daily" else 4, max_folds=3),
    )

    model_artifacts = train_and_save_models(
        train_df=hist_feat,
        target_cols=target_cols,
        axis=axis,
        freq=freq,
        model_dir=models_dir,
    )

    tipos = sorted(hist_df["tipo_servicio"].dropna().unique()) if axis == "service" else ["ALL"]
    future_base = _build_future_frame(cutoff, horizon_days, freq=freq, axis=axis, tipo_servicios=tipos)
    future_feat = add_time_features(future_base, holidays_df=holidays_df, weather_daily=weather_daily)

    pred = predict_targets_wide(future_feat, model_artifacts=model_artifacts)
    pred["freq"] = freq
    pred["axis"] = axis

    model_paths = collect_model_paths(model_artifacts)
    return pred, backtest, model_paths


def _wape(y_true: pd.Series, y_pred: pd.Series) -> float:
    yt = pd.to_numeric(y_true, errors="coerce").fillna(0.0).to_numpy(dtype=float)
    yp = pd.to_numeric(y_pred, errors="coerce").fillna(0.0).to_numpy(dtype=float)
    denom = np.sum(np.abs(yt))
    if denom <= 1e-12:
        return np.nan
    return float(np.sum(np.abs(yt - yp)) / denom)


def _build_expected_alignment_backtest(
    expected_hist_daily: pd.DataFrame,
    workload_daily_hist: pd.DataFrame,
) -> pd.DataFrame:
    if expected_hist_daily.empty or workload_daily_hist.empty:
        return pd.DataFrame()

    exp = expected_hist_daily.copy()
    exp["date"] = pd.to_datetime(exp["date"], errors="coerce").dt.normalize()

    real_target = (
        "picking_movs_atribuibles_entrega"
        if "picking_movs_atribuibles_entrega" in workload_daily_hist.columns
        else "picking_movs_atribuibles"
    )
    real = workload_daily_hist[["date", real_target]].copy()
    real = real.rename(columns={real_target: "picking_real_entrega"})
    real["date"] = pd.to_datetime(real["date"], errors="coerce").dt.normalize()

    daily = real.merge(
        exp[["date", "picking_movs_esperados_desde_servicio_p50", "picking_movs_esperados_desde_servicio_p80"]],
        on="date",
        how="inner",
    )
    if daily.empty:
        return pd.DataFrame()

    rows = []
    for pred_col, model_name in [
        ("picking_movs_esperados_desde_servicio_p50", "expected_picking_p50"),
        ("picking_movs_esperados_desde_servicio_p80", "expected_picking_p80"),
    ]:
        rows.append(
            {
                "axis": "workload_expected_from_service",
                "freq": "daily",
                "target": "picking_movs_atribuibles_entrega",
                "model": model_name,
                "fold_id": 0,
                "fold_start": daily["date"].min(),
                "fold_end": daily["date"].max(),
                "wape": _wape(daily["picking_real_entrega"], daily[pred_col]),
                "smape": np.nan,
                "mase": np.nan,
                "wape_peak5": np.nan,
                "smape_peak5": np.nan,
                "mase_peak5": np.nan,
                "coverage_empirical": (
                    float((daily["picking_real_entrega"] <= daily[pred_col]).mean())
                    if pred_col.endswith("_p80")
                    else np.nan
                ),
            }
        )

    d2 = daily.copy()
    d2["week_start"] = d2["date"] - pd.to_timedelta(d2["date"].dt.dayofweek, unit="D")
    weekly = (
        d2.groupby("week_start", dropna=False)
        .agg(
            picking_real_entrega=("picking_real_entrega", "sum"),
            picking_movs_esperados_desde_servicio_p50=("picking_movs_esperados_desde_servicio_p50", "sum"),
            picking_movs_esperados_desde_servicio_p80=("picking_movs_esperados_desde_servicio_p80", "sum"),
        )
        .reset_index()
    )
    for pred_col, model_name in [
        ("picking_movs_esperados_desde_servicio_p50", "expected_picking_p50"),
        ("picking_movs_esperados_desde_servicio_p80", "expected_picking_p80"),
    ]:
        rows.append(
            {
                "axis": "workload_expected_from_service",
                "freq": "weekly",
                "target": "picking_movs_atribuibles_entrega",
                "model": model_name,
                "fold_id": 0,
                "fold_start": weekly["week_start"].min(),
                "fold_end": weekly["week_start"].max(),
                "wape": _wape(weekly["picking_real_entrega"], weekly[pred_col]),
                "smape": np.nan,
                "mase": np.nan,
                "wape_peak5": np.nan,
                "smape_peak5": np.nan,
                "mase_peak5": np.nan,
                "coverage_empirical": (
                    float((weekly["picking_real_entrega"] <= weekly[pred_col]).mean())
                    if pred_col.endswith("_p80")
                    else np.nan
                ),
            }
        )

    return pd.DataFrame(rows)


def _build_forecast_daily_business(forecast_daily: pd.DataFrame) -> pd.DataFrame:
    if forecast_daily.empty:
        return pd.DataFrame()

    fd = forecast_daily.copy()
    fd["date"] = pd.to_datetime(fd["date"], errors="coerce").dt.normalize()

    svc_cols = [
        "conteo_servicios_p50",
        "conteo_servicios_p80",
        "m3_out_p50",
        "m3_out_p80",
        "pales_out_p50",
        "pales_out_p80",
        "cajas_out_p50",
        "cajas_out_p80",
        "peso_facturable_out_p50",
        "peso_facturable_out_p80",
        "m3_in_p50",
        "m3_in_p80",
        "pales_in_p50",
        "pales_in_p80",
        "cajas_in_p50",
        "cajas_in_p80",
        "peso_facturable_in_p50",
        "peso_facturable_in_p80",
    ]
    svc_src = fd[fd["axis"].eq("service")].copy()
    svc_src["tipo_servicio"] = svc_src.get("tipo_servicio", "desconocida").map(_normalize_service_type)
    for c in svc_cols:
        if c not in svc_src.columns:
            svc_src[c] = np.nan

    ent = (
        svc_src[svc_src["tipo_servicio"].map(_is_delivery_type)]
        .groupby("date", dropna=False)
        .agg(
            eventos_entrega_p50=("conteo_servicios_p50", "sum"),
            eventos_entrega_p80=("conteo_servicios_p80", "sum"),
            m3_out_p50=("m3_out_p50", "sum"),
            m3_out_p80=("m3_out_p80", "sum"),
            pales_out_p50=("pales_out_p50", "sum"),
            pales_out_p80=("pales_out_p80", "sum"),
            cajas_out_p50=("cajas_out_p50", "sum"),
            cajas_out_p80=("cajas_out_p80", "sum"),
            peso_facturable_out_p50=("peso_facturable_out_p50", "sum"),
            peso_facturable_out_p80=("peso_facturable_out_p80", "sum"),
        )
        .reset_index()
    )
    rec = (
        svc_src[svc_src["tipo_servicio"].map(_is_pickup_type)]
        .groupby("date", dropna=False)
        .agg(
            eventos_recogida_p50=("conteo_servicios_p50", "sum"),
            eventos_recogida_p80=("conteo_servicios_p80", "sum"),
            m3_in_p50=("m3_in_p50", "sum"),
            m3_in_p80=("m3_in_p80", "sum"),
            pales_in_p50=("pales_in_p50", "sum"),
            pales_in_p80=("pales_in_p80", "sum"),
            cajas_in_p50=("cajas_in_p50", "sum"),
            cajas_in_p80=("cajas_in_p80", "sum"),
            peso_facturable_in_p50=("peso_facturable_in_p50", "sum"),
            peso_facturable_in_p80=("peso_facturable_in_p80", "sum"),
        )
        .reset_index()
    )
    svc = ent.merge(rec, on="date", how="outer")

    exp_cols = ["picking_movs_esperados_desde_servicio_p50", "picking_movs_esperados_desde_servicio_p80"]
    exp_src = fd[fd["axis"].eq("workload_expected_from_service")].copy()
    for c in exp_cols:
        if c not in exp_src.columns:
            exp_src[c] = np.nan
    exp = (
        exp_src.groupby("date", dropna=False)
        .agg(
            picking_movs_esperados_p50=("picking_movs_esperados_desde_servicio_p50", "sum"),
            picking_movs_esperados_p80=("picking_movs_esperados_desde_servicio_p80", "sum"),
        )
        .reset_index()
    )

    out = svc.merge(exp, on="date", how="outer").sort_values("date")
    out = out.rename(columns={"date": "fecha"})
    required = [
        "fecha",
        "eventos_entrega_p50",
        "eventos_entrega_p80",
        "m3_out_p50",
        "m3_out_p80",
        "pales_out_p50",
        "pales_out_p80",
        "cajas_out_p50",
        "cajas_out_p80",
        "peso_facturable_out_p50",
        "peso_facturable_out_p80",
        "eventos_recogida_p50",
        "eventos_recogida_p80",
        "m3_in_p50",
        "m3_in_p80",
        "pales_in_p50",
        "pales_in_p80",
        "cajas_in_p50",
        "cajas_in_p80",
        "peso_facturable_in_p50",
        "peso_facturable_in_p80",
        "picking_movs_esperados_p50",
        "picking_movs_esperados_p80",
    ]
    for c in required:
        if c not in out.columns:
            out[c] = np.nan
    out = out[required]
    return out


def _build_forecast_weekly_business(
    forecast_weekly: pd.DataFrame,
    forecast_daily: pd.DataFrame,
    workload_daily_hist: pd.DataFrame,
) -> pd.DataFrame:
    if forecast_weekly.empty and forecast_daily.empty:
        return pd.DataFrame()

    fw = forecast_weekly.copy()
    fw["date"] = pd.to_datetime(fw["date"], errors="coerce").dt.normalize()

    fd = forecast_daily.copy()
    fd["date"] = pd.to_datetime(fd["date"], errors="coerce").dt.normalize()

    service_src = fw[fw["axis"].eq("service")].copy()
    service_src["tipo_servicio"] = service_src.get("tipo_servicio", "desconocida").map(_normalize_service_type)
    for c in [
        "conteo_servicios_p50",
        "conteo_servicios_p80",
        "m3_out_p50",
        "m3_out_p80",
        "pales_out_p50",
        "pales_out_p80",
        "cajas_out_p50",
        "cajas_out_p80",
        "peso_facturable_out_p50",
        "peso_facturable_out_p80",
        "m3_in_p50",
        "m3_in_p80",
        "pales_in_p50",
        "pales_in_p80",
        "cajas_in_p50",
        "cajas_in_p80",
        "peso_facturable_in_p50",
        "peso_facturable_in_p80",
    ]:
        if c not in service_src.columns:
            service_src[c] = np.nan

    ent_w = (
        service_src[service_src["tipo_servicio"].map(_is_delivery_type)]
        .groupby("date", dropna=False)
        .agg(
            eventos_entrega_semana_p50=("conteo_servicios_p50", "sum"),
            eventos_entrega_semana_p80=("conteo_servicios_p80", "sum"),
            m3_out_semana_p50=("m3_out_p50", "sum"),
            m3_out_semana_p80=("m3_out_p80", "sum"),
            pales_out_semana_p50=("pales_out_p50", "sum"),
            pales_out_semana_p80=("pales_out_p80", "sum"),
            cajas_out_semana_p50=("cajas_out_p50", "sum"),
            cajas_out_semana_p80=("cajas_out_p80", "sum"),
            peso_facturable_out_semana_p50=("peso_facturable_out_p50", "sum"),
            peso_facturable_out_semana_p80=("peso_facturable_out_p80", "sum"),
        )
        .reset_index()
    )
    rec_w = (
        service_src[service_src["tipo_servicio"].map(_is_pickup_type)]
        .groupby("date", dropna=False)
        .agg(
            eventos_recogida_semana_p50=("conteo_servicios_p50", "sum"),
            eventos_recogida_semana_p80=("conteo_servicios_p80", "sum"),
            m3_in_semana_p50=("m3_in_p50", "sum"),
            m3_in_semana_p80=("m3_in_p80", "sum"),
            pales_in_semana_p50=("pales_in_p50", "sum"),
            pales_in_semana_p80=("pales_in_p80", "sum"),
            cajas_in_semana_p50=("cajas_in_p50", "sum"),
            cajas_in_semana_p80=("cajas_in_p80", "sum"),
            peso_facturable_in_semana_p50=("peso_facturable_in_p50", "sum"),
            peso_facturable_in_semana_p80=("peso_facturable_in_p80", "sum"),
        )
        .reset_index()
    )
    service_w = (
        ent_w.merge(rec_w, on="date", how="outer")
        .rename(columns={"date": "week_start_date"})
    )

    exp_d = fd[fd["axis"].eq("workload_expected_from_service")].copy()
    for c in ["picking_movs_esperados_desde_servicio_p50", "picking_movs_esperados_desde_servicio_p80"]:
        if c not in exp_d.columns:
            exp_d[c] = np.nan
    exp_d["week_start_date"] = exp_d["date"] - pd.to_timedelta(exp_d["date"].dt.dayofweek, unit="D")
    exp_w = (
        exp_d.groupby("week_start_date", dropna=False)
        .agg(
            picking_movs_esperados_semana_p50=("picking_movs_esperados_desde_servicio_p50", "sum"),
            picking_movs_esperados_semana_p80=("picking_movs_esperados_desde_servicio_p80", "sum"),
        )
        .reset_index()
    )

    wl_src = fw[fw["axis"].eq("workload")].copy()
    for c in ["picking_movs_no_atribuibles_p50", "picking_movs_no_atribuibles_p80"]:
        if c not in wl_src.columns:
            wl_src[c] = np.nan
    wl_w = (
        wl_src
        .groupby("date", dropna=False)
        .agg(
            picking_movs_no_atribuibles_semana_p50=("picking_movs_no_atribuibles_p50", "sum"),
            picking_movs_no_atribuibles_semana_p80=("picking_movs_no_atribuibles_p80", "sum"),
        )
        .reset_index()
        .rename(columns={"date": "week_start_date"})
    )

    wr = workload_daily_hist.copy()
    real_col = (
        "picking_movs_atribuibles_entrega"
        if "picking_movs_atribuibles_entrega" in wr.columns
        else "picking_movs_atribuibles"
    )
    wr["week_start_date"] = pd.to_datetime(wr["date"], errors="coerce").dt.normalize() - pd.to_timedelta(
        pd.to_datetime(wr["date"], errors="coerce").dt.dayofweek, unit="D"
    )
    wr_w = (
        wr.groupby("week_start_date", dropna=False)
        .agg(
            picking_movs_reales_semana=(real_col, "sum"),
            picking_movs_no_atribuibles_semana_hist=("picking_movs_no_atribuibles", "sum"),
        )
        .reset_index()
    )

    week_index = pd.concat(
        [
            service_w[["week_start_date"]] if not service_w.empty else pd.DataFrame(columns=["week_start_date"]),
            exp_w[["week_start_date"]] if not exp_w.empty else pd.DataFrame(columns=["week_start_date"]),
            wl_w[["week_start_date"]] if not wl_w.empty else pd.DataFrame(columns=["week_start_date"]),
            wr_w[["week_start_date"]] if not wr_w.empty else pd.DataFrame(columns=["week_start_date"]),
        ],
        ignore_index=True,
    ).drop_duplicates()

    out = week_index.merge(service_w, on="week_start_date", how="left")
    out = out.merge(exp_w, on="week_start_date", how="left")
    out = out.merge(wl_w, on="week_start_date", how="left")
    out = out.merge(wr_w, on="week_start_date", how="left")

    out["week_start_date"] = pd.to_datetime(out["week_start_date"], errors="coerce").dt.normalize()
    out = out.sort_values("week_start_date").reset_index(drop=True)

    iso = out["week_start_date"].dt.isocalendar()
    out["week_iso"] = iso.week.astype("Int64")
    out["year"] = iso.year.astype("Int64")
    out["week_end_date"] = out["week_start_date"] + pd.Timedelta(days=6)
    out["picking_movs_no_atribuibles_semana"] = out.get("picking_movs_no_atribuibles_semana_hist").combine_first(
        out.get("picking_movs_no_atribuibles_semana_p50")
    )

    ordered_cols = [
        "week_iso",
        "year",
        "week_start_date",
        "week_end_date",
        "eventos_entrega_semana_p50",
        "eventos_entrega_semana_p80",
        "m3_out_semana_p50",
        "m3_out_semana_p80",
        "pales_out_semana_p50",
        "pales_out_semana_p80",
        "cajas_out_semana_p50",
        "cajas_out_semana_p80",
        "peso_facturable_out_semana_p50",
        "peso_facturable_out_semana_p80",
        "eventos_recogida_semana_p50",
        "eventos_recogida_semana_p80",
        "m3_in_semana_p50",
        "m3_in_semana_p80",
        "pales_in_semana_p50",
        "pales_in_semana_p80",
        "cajas_in_semana_p50",
        "cajas_in_semana_p80",
        "peso_facturable_in_semana_p50",
        "peso_facturable_in_semana_p80",
        "picking_movs_esperados_semana_p50",
        "picking_movs_esperados_semana_p80",
        "picking_movs_reales_semana",
        "picking_movs_no_atribuibles_semana",
        "picking_movs_no_atribuibles_semana_p50",
        "picking_movs_no_atribuibles_semana_p80",
    ]
    for c in ordered_cols:
        if c not in out.columns:
            out[c] = np.nan
    return out[ordered_cols]


def main() -> None:
    _setup_logging()
    args = parse_args()

    root = Path(".").resolve()
    outputs_dir = root / "outputs"
    diag_dir = outputs_dir / "diagnostics"
    models_dir = root / "models"
    outputs_dir.mkdir(exist_ok=True)
    diag_dir.mkdir(exist_ok=True, parents=True)
    models_dir.mkdir(exist_ok=True)

    cutoff = pd.Timestamp(args.cutoff_date).normalize() if args.cutoff_date else pd.Timestamp.today().normalize()
    use_weather = parse_bool(args.use_weather)
    debug_join = parse_bool(args.debug_join)
    exclude_years = parse_exclude_years(args.exclude_years)

    LOGGER.info("Cutoff date: %s", cutoff.date())
    LOGGER.info("Exclude years train/scoring: %s", exclude_years)

    paths = resolve_input_paths(root)
    raw_alb = load_albaranes(paths.albaranes)
    raw_mov = load_movimientos(paths.movimientos)
    holidays_df = load_holidays(paths.holidays)
    prov_station_map = load_provincia_station_map(paths.provincia_station_map)

    albaranes = clean_albaranes(raw_alb, cutoff_date=cutoff)
    movimientos = clean_movimientos(raw_mov)

    if debug_join:
        generate_join_debug_outputs(albaranes=albaranes, movimientos=movimientos, outputs_dir=outputs_dir)
        LOGGER.info("Modo debug_join completado. Salida: %s", outputs_dir)
        return

    albaranes["provincia_norm"] = normalize_provincia_destino(albaranes["provincia_destino"], prov_station_map)

    service_level = build_service_level(albaranes)
    service_hist_for_join = service_level[service_level["fecha_servicio"] <= cutoff].copy()

    join_out = assign_movements_to_services(
        movements=movimientos,
        services=service_hist_for_join,
        window_days=args.assignment_window_days,
    )

    service_daily, service_weekly_initial = build_service_targets(service_level)
    workload_daily, workload_weekly_initial = build_workload_targets(join_out.movements_joined)
    service_daily = service_daily.drop(columns=["mix_entrega_vs_recogida"], errors="ignore")
    service_weekly_initial = service_weekly_initial.drop(columns=["mix_entrega_vs_recogida"], errors="ignore")

    service_daily["excluded_year"] = service_daily["date"].dt.year.isin(exclude_years).astype(int)
    workload_daily["excluded_year"] = workload_daily["date"].dt.year.isin(exclude_years).astype(int)

    service_daily_hist = service_daily[(service_daily["date"] <= cutoff) & service_daily["excluded_year"].eq(0)].copy()
    workload_daily_hist = workload_daily[(workload_daily["date"] <= cutoff) & workload_daily["excluded_year"].eq(0)].copy()

    service_weekly_hist = _aggregate_weekly_from_daily(service_daily_hist, SERVICE_TARGETS, axis="service")
    workload_weekly_hist = _aggregate_weekly_from_daily(workload_daily_hist, WORKLOAD_TARGETS, axis="workload")

    weather = build_weighted_weather(
        service_level=service_level,
        provincia_station_map=prov_station_map,
        outputs_dir=outputs_dir,
        use_weather=use_weather,
    )

    do_daily = args.freq in {"daily", "both"}
    do_weekly = args.freq in {"weekly", "both"}

    forecast_daily_parts = []
    forecast_weekly_parts = []
    all_backtests = []
    all_model_paths = []
    expected_alignment_bt = pd.DataFrame()
    expected_hist_daily = pd.DataFrame()

    if do_daily:
        svc_pred_d, svc_bt_d, svc_models_d = run_model_block(
            hist_df=service_daily_hist,
            all_df=service_daily,
            holidays_df=holidays_df,
            weather_daily=weather.weighted_daily,
            axis="service",
            freq="daily",
            targets=SERVICE_TARGETS,
            cutoff=cutoff,
            horizon_days=args.horizon_days,
            models_dir=models_dir,
        )
        wl_pred_d, wl_bt_d, wl_models_d = run_model_block(
            hist_df=workload_daily_hist,
            all_df=workload_daily,
            holidays_df=holidays_df,
            weather_daily=weather.weighted_daily,
            axis="workload",
            freq="daily",
            targets=WORKLOAD_TARGETS,
            cutoff=cutoff,
            horizon_days=args.horizon_days,
            models_dir=models_dir,
        )

        expected_from_service = transform_service_forecast_to_workload_expected(
            service_forecast_daily=svc_pred_d,
            join_movements=join_out.movements_joined,
            lead_time_summary=join_out.lead_time_summary,
            holidays_df=holidays_df,
            service_level_hist=service_hist_for_join,
        )
        expected_from_service["freq"] = "daily"

        # Backtest rapido adicional: expected vs movs_atribuibles reales historicos.
        hist_expected_input = service_daily_hist[["date", "axis", "tipo_servicio", "conteo_servicios", "m3_out", "m3_in"]].copy()
        hist_expected_input["conteo_servicios_p50"] = hist_expected_input["conteo_servicios"]
        hist_expected_input["conteo_servicios_p80"] = hist_expected_input["conteo_servicios"]
        hist_expected_input["m3_out_p50"] = hist_expected_input["m3_out"]
        hist_expected_input["m3_out_p80"] = hist_expected_input["m3_out"]
        hist_expected_input["m3_in_p50"] = hist_expected_input["m3_in"]
        hist_expected_input["m3_in_p80"] = hist_expected_input["m3_in"]

        expected_hist_daily = transform_service_forecast_to_workload_expected(
            service_forecast_daily=hist_expected_input,
            join_movements=join_out.movements_joined,
            lead_time_summary=join_out.lead_time_summary,
            holidays_df=holidays_df,
            service_level_hist=service_hist_for_join,
        )
        expected_alignment_bt = _build_expected_alignment_backtest(expected_hist_daily, workload_daily_hist)

        forecast_daily_parts.extend([svc_pred_d, wl_pred_d, expected_from_service])
        all_backtests.extend([svc_bt_d, wl_bt_d])
        all_model_paths.extend([svc_models_d, wl_models_d])

    if do_weekly:
        svc_pred_w, svc_bt_w, svc_models_w = run_model_block(
            hist_df=service_weekly_hist,
            all_df=service_weekly_initial,
            holidays_df=holidays_df,
            weather_daily=weather.weighted_daily,
            axis="service",
            freq="weekly",
            targets=SERVICE_TARGETS,
            cutoff=cutoff,
            horizon_days=args.horizon_days,
            models_dir=models_dir,
        )
        wl_pred_w, wl_bt_w, wl_models_w = run_model_block(
            hist_df=workload_weekly_hist,
            all_df=workload_weekly_initial,
            holidays_df=holidays_df,
            weather_daily=weather.weighted_daily,
            axis="workload",
            freq="weekly",
            targets=WORKLOAD_TARGETS,
            cutoff=cutoff,
            horizon_days=args.horizon_days,
            models_dir=models_dir,
        )

        forecast_weekly_parts.extend([svc_pred_w, wl_pred_w])
        all_backtests.extend([svc_bt_w, wl_bt_w])
        all_model_paths.extend([svc_models_w, wl_models_w])

    forecast_daily = pd.concat(forecast_daily_parts, ignore_index=True) if forecast_daily_parts else pd.DataFrame()
    forecast_weekly = pd.concat(forecast_weekly_parts, ignore_index=True) if forecast_weekly_parts else pd.DataFrame()

    if forecast_weekly.empty and not forecast_daily.empty:
        numeric_pred_cols = [c for c in forecast_daily.columns if c.endswith("_p50") or c.endswith("_p80")]
        tmp = forecast_daily.copy()
        tmp["week_start"] = pd.to_datetime(tmp["date"]).dt.normalize() - pd.to_timedelta(pd.to_datetime(tmp["date"]).dt.dayofweek, unit="D")
        group_cols = ["week_start"]
        for c in ["axis", "tipo_servicio", "freq"]:
            if c in tmp.columns:
                group_cols.append(c)
        forecast_weekly = tmp.groupby(group_cols, dropna=False)[numeric_pred_cols].sum().reset_index().rename(columns={"week_start": "date"})
        forecast_weekly["freq"] = "weekly"

    forecast_daily = _post_process_forecasts(forecast_daily)
    forecast_weekly = _post_process_forecasts(forecast_weekly)

    backtest_df = pd.concat([b for b in all_backtests if b is not None and not b.empty], ignore_index=True) if all_backtests else pd.DataFrame()
    if not expected_alignment_bt.empty:
        backtest_df = pd.concat([backtest_df, expected_alignment_bt], ignore_index=True)

    model_paths_df = pd.concat(all_model_paths, ignore_index=True) if all_model_paths else pd.DataFrame()

    forecast_daily_business = _build_forecast_daily_business(forecast_daily)
    forecast_weekly_business = _build_forecast_weekly_business(forecast_weekly, forecast_daily, workload_daily_hist)
    forecast_daily_business = _post_process_forecasts(forecast_daily_business)
    forecast_weekly_business = _post_process_forecasts(forecast_weekly_business)

    forecast_daily.to_csv(outputs_dir / "forecast_daily.csv", index=False)
    forecast_weekly.to_csv(outputs_dir / "forecast_weekly.csv", index=False)
    forecast_daily_business.to_csv(outputs_dir / "forecast_daily_business.csv", index=False)
    forecast_weekly_business.to_csv(outputs_dir / "forecast_weekly_business.csv", index=False)
    backtest_df.to_csv(outputs_dir / "backtest_metrics.csv", index=False)
    join_out.join_kpis.to_csv(outputs_dir / "join_kpis.csv", index=False)
    join_out.lead_time_summary.to_csv(outputs_dir / "lead_time_summary.csv", index=False)
    model_paths_df.to_csv(outputs_dir / "model_registry.csv", index=False)

    save_backtest_plots(backtest_df, out_dir=diag_dir)
    save_forecast_plots(
        pd.concat([forecast_daily.assign(freq="daily"), forecast_weekly.assign(freq="weekly")], ignore_index=True)
        if not forecast_daily.empty or not forecast_weekly.empty
        else pd.DataFrame(),
        out_dir=diag_dir,
    )
    save_picking_validation_plot(expected_hist_daily, workload_daily_hist, out_dir=diag_dir)

    LOGGER.info("Pipeline completado")
    LOGGER.info("Salida: %s", outputs_dir)


if __name__ == "__main__":
    main()
