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
from src.join_assignment import assign_movements_to_services
from src.predict import collect_model_paths, predict_targets_wide
from src.report import save_backtest_plots, save_forecast_plots
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
    "mix_entrega_vs_recogida",
]

WORKLOAD_TARGETS = [
    "conteo_movimientos",
    "unidades_movidas",
    "skus_distintos",
    "workload_atribuible_movs",
    "workload_no_atribuible_movs",
]


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "si"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline forecasting logistica (servicio + workload)")
    parser.add_argument("--horizon_days", type=int, default=60)
    parser.add_argument("--freq", type=str, default="both", choices=["daily", "weekly", "both"])
    parser.add_argument("--use_weather", type=str, default="true")
    parser.add_argument("--assignment_window_days", type=int, default=30)
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
    exclude_years = parse_exclude_years(args.exclude_years)

    LOGGER.info("Cutoff date: %s", cutoff.date())
    LOGGER.info("Exclude years train/scoring: %s", exclude_years)

    paths = resolve_input_paths(root)
    raw_alb = load_albaranes(paths.albaranes)
    raw_mov = load_movimientos(paths.movimientos)
    holidays_df = load_holidays(paths.holidays)
    prov_station_map = load_provincia_station_map(paths.provincia_station_map)

    albaranes = clean_albaranes(raw_alb, cutoff_date=cutoff)
    albaranes["provincia_norm"] = normalize_provincia_destino(albaranes["provincia_destino"], prov_station_map)

    movimientos = clean_movimientos(raw_mov)

    service_level = build_service_level(albaranes)
    service_hist_for_join = service_level[service_level["fecha_servicio"] <= cutoff].copy()

    join_out = assign_movements_to_services(
        movements=movimientos,
        services=service_hist_for_join,
        window_days=args.assignment_window_days,
    )

    service_daily, service_weekly_initial = build_service_targets(service_level)
    workload_daily, workload_weekly_initial = build_workload_targets(join_out.movements_joined)

    # Exclusión configurable por año para entrenamiento/scoring
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

        # Transformación A->B esperada (planificación)
        expected_from_service = transform_service_forecast_to_workload_expected(
            service_forecast_daily=svc_pred_d,
            join_movements=join_out.movements_joined,
            lead_time_summary=join_out.lead_time_summary,
            holidays_df=holidays_df,
        )
        expected_from_service["freq"] = "daily"

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

    # Si no hubo forecast semanal explícito, derivarlo del diario.
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

    backtest_df = pd.concat([b for b in all_backtests if b is not None and not b.empty], ignore_index=True) if all_backtests else pd.DataFrame()
    model_paths_df = pd.concat(all_model_paths, ignore_index=True) if all_model_paths else pd.DataFrame()

    # Exports obligatorios
    forecast_daily.to_csv(outputs_dir / "forecast_daily.csv", index=False)
    forecast_weekly.to_csv(outputs_dir / "forecast_weekly.csv", index=False)
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

    LOGGER.info("Pipeline completado")
    LOGGER.info("Salida: %s", outputs_dir)


if __name__ == "__main__":
    main()
