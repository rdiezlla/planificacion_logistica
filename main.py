from __future__ import annotations

import argparse
import logging
import math
from pathlib import Path

import numpy as np
import pandas as pd

from src.backtest import BacktestConfig, run_backtest
from src.cleaning import clean_albaranes, clean_movimientos
from src.canonical_services import CUTOVER_DATE, build_canonical_layer
from src.feature_engineering import add_time_features, parse_exclude_years
from src.geo_normalization import normalize_provincia_destino
from src.io import (
    load_albaranes,
    load_holidays,
    load_movimientos,
    load_operational_orders,
    load_provincia_station_map,
    resolve_input_paths,
)
from src.join_assignment import assign_movements_to_services, generate_join_debug_outputs
from src.monitoring import (
    build_feature_policy_summary,
    build_model_health_summary,
    build_service_intensity_summary,
    build_service_type_audit,
)
from src.predict import collect_model_paths, predict_targets_wide
from src.report import save_backtest_plots, save_forecast_plots, save_picking_validation_plot
from src.staffing import build_staffing_plan, load_labor_standards
from src.supervisor_dashboard import (
    build_supervisor_dashboard_daily,
    build_supervisor_dashboard_weekly,
)
from src.targets import (
    build_service_targets,
    build_workload_targets,
    densify_daily_calendar,
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
LABOR_STANDARDS_PATH = Path("data") / "labor_standards.csv"


def _normalize_service_type(value: object) -> str:
    if value is None:
        return "desconocida"
    t = str(value).strip().lower()
    return t if t else "desconocida"


def _is_delivery_type(value: object) -> bool:
    return _normalize_service_type(value).startswith("entrega")


def _is_pickup_type(value: object) -> bool:
    return _normalize_service_type(value).startswith("recogida")


def _is_delivery_component_type(value: object) -> bool:
    t = _normalize_service_type(value)
    return t.startswith("entrega") or t == "mixto"


def _is_pickup_component_type(value: object) -> bool:
    t = _normalize_service_type(value)
    return t.startswith("recogida") or t == "mixto"


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
    parser.add_argument(
        "--data_mode",
        type=str,
        default="hybrid",
        choices=["legacy", "hybrid", "operational-first"],
    )
    parser.add_argument(
        "--operational_cutover_date",
        type=str,
        default=str(CUTOVER_DATE.date()),
        help="Fecha de cutover para priorizar operativo (YYYY-MM-DD).",
    )
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
        config=BacktestConfig(
            min_train_days=180 if freq == "daily" else 30,
            test_days=28 if freq == "daily" else 8,
            step_days=28 if freq == "daily" else 4,
            max_folds=4,
        ),
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
        is_p80 = pred_col.endswith("_p80")
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
                "empirical_coverage_p80": (
                    float((daily["picking_real_entrega"] <= daily[pred_col]).mean())
                    if is_p80
                    else np.nan
                ),
                "pinball_loss_p50": (
                    float(np.mean(np.maximum(0.5 * (daily["picking_real_entrega"] - daily[pred_col]), -0.5 * (daily["picking_real_entrega"] - daily[pred_col]))))
                    if not is_p80
                    else np.nan
                ),
                "pinball_loss_p80": (
                    float(np.mean(np.maximum(0.8 * (daily["picking_real_entrega"] - daily[pred_col]), -0.2 * (daily["picking_real_entrega"] - daily[pred_col]))))
                    if is_p80
                    else np.nan
                ),
                "coverage_empirical": (
                    float((daily["picking_real_entrega"] <= daily[pred_col]).mean())
                    if is_p80
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
        is_p80 = pred_col.endswith("_p80")
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
                "empirical_coverage_p80": (
                    float((weekly["picking_real_entrega"] <= weekly[pred_col]).mean())
                    if is_p80
                    else np.nan
                ),
                "pinball_loss_p50": (
                    float(np.mean(np.maximum(0.5 * (weekly["picking_real_entrega"] - weekly[pred_col]), -0.5 * (weekly["picking_real_entrega"] - weekly[pred_col]))))
                    if not is_p80
                    else np.nan
                ),
                "pinball_loss_p80": (
                    float(np.mean(np.maximum(0.8 * (weekly["picking_real_entrega"] - weekly[pred_col]), -0.2 * (weekly["picking_real_entrega"] - weekly[pred_col]))))
                    if is_p80
                    else np.nan
                ),
                "coverage_empirical": (
                    float((weekly["picking_real_entrega"] <= weekly[pred_col]).mean())
                    if is_p80
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
        svc_src[svc_src["tipo_servicio"].map(_is_delivery_component_type)]
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
        svc_src[svc_src["tipo_servicio"].map(_is_pickup_component_type)]
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
    inbound_cols = [
        "inbound_recepcion_pales_esperados_p50",
        "inbound_recepcion_pales_esperados_p80",
        "inbound_recepcion_cr_esperados_p50",
        "inbound_recepcion_cr_esperados_p80",
        "inbound_ubicacion_cajas_esperados_p50",
        "inbound_ubicacion_cajas_esperados_p80",
        "inbound_ubicacion_ep_esperados_p50",
        "inbound_ubicacion_ep_esperados_p80",
        "inbound_m3_esperados_p50",
        "inbound_m3_esperados_p80",
    ]
    for c in exp_cols + inbound_cols:
        if c not in exp_src.columns:
            exp_src[c] = np.nan
    exp = (
        exp_src.groupby("date", dropna=False)
        .agg(
            picking_movs_esperados_p50=("picking_movs_esperados_desde_servicio_p50", "sum"),
            picking_movs_esperados_p80=("picking_movs_esperados_desde_servicio_p80", "sum"),
            inbound_recepcion_pales_esperados_p50=("inbound_recepcion_pales_esperados_p50", "sum"),
            inbound_recepcion_pales_esperados_p80=("inbound_recepcion_pales_esperados_p80", "sum"),
            inbound_recepcion_cr_esperados_p50=("inbound_recepcion_cr_esperados_p50", "sum"),
            inbound_recepcion_cr_esperados_p80=("inbound_recepcion_cr_esperados_p80", "sum"),
            inbound_ubicacion_cajas_esperados_p50=("inbound_ubicacion_cajas_esperados_p50", "sum"),
            inbound_ubicacion_cajas_esperados_p80=("inbound_ubicacion_cajas_esperados_p80", "sum"),
            inbound_ubicacion_ep_esperados_p50=("inbound_ubicacion_ep_esperados_p50", "sum"),
            inbound_ubicacion_ep_esperados_p80=("inbound_ubicacion_ep_esperados_p80", "sum"),
            inbound_m3_esperados_p50=("inbound_m3_esperados_p50", "sum"),
            inbound_m3_esperados_p80=("inbound_m3_esperados_p80", "sum"),
        )
        .reset_index()
    )

    wl_src = fd[fd["axis"].eq("workload")].copy()
    for c in ["picking_movs_no_atribuibles_p50", "picking_movs_no_atribuibles_p80"]:
        if c not in wl_src.columns:
            wl_src[c] = np.nan
    wl = (
        wl_src.groupby("date", dropna=False)
        .agg(
            picking_movs_no_atribuibles_p50=("picking_movs_no_atribuibles_p50", "sum"),
            picking_movs_no_atribuibles_p80=("picking_movs_no_atribuibles_p80", "sum"),
        )
        .reset_index()
    )

    out = svc.merge(exp, on="date", how="outer").merge(wl, on="date", how="outer").sort_values("date")
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
        "inbound_recepcion_pales_esperados_p50",
        "inbound_recepcion_pales_esperados_p80",
        "inbound_recepcion_cr_esperados_p50",
        "inbound_recepcion_cr_esperados_p80",
        "inbound_ubicacion_cajas_esperados_p50",
        "inbound_ubicacion_cajas_esperados_p80",
        "inbound_ubicacion_ep_esperados_p50",
        "inbound_ubicacion_ep_esperados_p80",
        "inbound_m3_esperados_p50",
        "inbound_m3_esperados_p80",
        "picking_movs_no_atribuibles_p50",
        "picking_movs_no_atribuibles_p80",
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
        service_src[service_src["tipo_servicio"].map(_is_delivery_component_type)]
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
        service_src[service_src["tipo_servicio"].map(_is_pickup_component_type)]
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
    for c in [
        "picking_movs_esperados_desde_servicio_p50",
        "picking_movs_esperados_desde_servicio_p80",
        "inbound_recepcion_pales_esperados_p50",
        "inbound_recepcion_pales_esperados_p80",
        "inbound_recepcion_cr_esperados_p50",
        "inbound_recepcion_cr_esperados_p80",
        "inbound_ubicacion_cajas_esperados_p50",
        "inbound_ubicacion_cajas_esperados_p80",
        "inbound_ubicacion_ep_esperados_p50",
        "inbound_ubicacion_ep_esperados_p80",
        "inbound_m3_esperados_p50",
        "inbound_m3_esperados_p80",
    ]:
        if c not in exp_d.columns:
            exp_d[c] = np.nan
    exp_d["week_start_date"] = exp_d["date"] - pd.to_timedelta(exp_d["date"].dt.dayofweek, unit="D")
    exp_w = (
        exp_d.groupby("week_start_date", dropna=False)
        .agg(
            picking_movs_esperados_semana_p50=("picking_movs_esperados_desde_servicio_p50", "sum"),
            picking_movs_esperados_semana_p80=("picking_movs_esperados_desde_servicio_p80", "sum"),
            inbound_recepcion_pales_esperados_semana_p50=("inbound_recepcion_pales_esperados_p50", "sum"),
            inbound_recepcion_pales_esperados_semana_p80=("inbound_recepcion_pales_esperados_p80", "sum"),
            inbound_recepcion_cr_esperados_semana_p50=("inbound_recepcion_cr_esperados_p50", "sum"),
            inbound_recepcion_cr_esperados_semana_p80=("inbound_recepcion_cr_esperados_p80", "sum"),
            inbound_ubicacion_cajas_esperados_semana_p50=("inbound_ubicacion_cajas_esperados_p50", "sum"),
            inbound_ubicacion_cajas_esperados_semana_p80=("inbound_ubicacion_cajas_esperados_p80", "sum"),
            inbound_ubicacion_ep_esperados_semana_p50=("inbound_ubicacion_ep_esperados_p50", "sum"),
            inbound_ubicacion_ep_esperados_semana_p80=("inbound_ubicacion_ep_esperados_p80", "sum"),
            inbound_m3_esperados_semana_p50=("inbound_m3_esperados_p50", "sum"),
            inbound_m3_esperados_semana_p80=("inbound_m3_esperados_p80", "sum"),
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
        "inbound_recepcion_pales_esperados_semana_p50",
        "inbound_recepcion_pales_esperados_semana_p80",
        "inbound_recepcion_cr_esperados_semana_p50",
        "inbound_recepcion_cr_esperados_semana_p80",
        "inbound_ubicacion_cajas_esperados_semana_p50",
        "inbound_ubicacion_cajas_esperados_semana_p80",
        "inbound_ubicacion_ep_esperados_semana_p50",
        "inbound_ubicacion_ep_esperados_semana_p80",
        "inbound_m3_esperados_semana_p50",
        "inbound_m3_esperados_semana_p80",
        "picking_movs_reales_semana",
        "picking_movs_no_atribuibles_semana",
        "picking_movs_no_atribuibles_semana_p50",
        "picking_movs_no_atribuibles_semana_p80",
    ]
    for c in ordered_cols:
        if c not in out.columns:
            out[c] = np.nan
    return out[ordered_cols]


def _build_workload_expected_daily(forecast_daily: pd.DataFrame) -> pd.DataFrame:
    if forecast_daily.empty:
        return pd.DataFrame()
    exp = forecast_daily[forecast_daily["axis"].eq("workload_expected_from_service")].copy()
    if exp.empty:
        return pd.DataFrame()
    exp["date"] = pd.to_datetime(exp["date"], errors="coerce").dt.normalize()
    required = [
        "date",
        "picking_movs_esperados_desde_servicio_p50",
        "picking_movs_esperados_desde_servicio_p80",
        "inbound_recepcion_pales_esperados_p50",
        "inbound_recepcion_pales_esperados_p80",
        "inbound_recepcion_cr_esperados_p50",
        "inbound_recepcion_cr_esperados_p80",
        "inbound_ubicacion_cajas_esperados_p50",
        "inbound_ubicacion_cajas_esperados_p80",
        "inbound_ubicacion_ep_esperados_p50",
        "inbound_ubicacion_ep_esperados_p80",
        "inbound_m3_esperados_p50",
        "inbound_m3_esperados_p80",
    ]
    for c in required:
        if c not in exp.columns:
            exp[c] = np.nan
    return exp[required].rename(columns={"date": "fecha"}).sort_values("fecha").reset_index(drop=True)


def _build_workload_expected_weekly(workload_expected_daily: pd.DataFrame) -> pd.DataFrame:
    if workload_expected_daily.empty:
        return pd.DataFrame()
    df = workload_expected_daily.copy()
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.normalize()
    df["week_start_date"] = df["fecha"] - pd.to_timedelta(df["fecha"].dt.dayofweek, unit="D")
    agg_cols = [c for c in df.columns if c.endswith("_p50") or c.endswith("_p80")]
    weekly = df.groupby("week_start_date", dropna=False)[agg_cols].sum().reset_index()
    iso = weekly["week_start_date"].dt.isocalendar()
    weekly["week_iso"] = iso.week.astype("Int64")
    weekly["year"] = iso.year.astype("Int64")
    weekly["week_end_date"] = weekly["week_start_date"] + pd.Timedelta(days=6)
    ordered = ["week_iso", "year", "week_start_date", "week_end_date"] + agg_cols
    return weekly[ordered].sort_values("week_start_date").reset_index(drop=True)


def _update_forecast_snapshot_registry(
    *,
    outputs_dir: Path,
    snapshot_date: str,
    cutoff_date: str,
    file_name: str,
    rows: int,
    created_ts: str,
) -> None:
    registry_path = outputs_dir / "forecast_snapshot_registry.csv"
    new_row = pd.DataFrame(
        [
            {
                "snapshot_date": snapshot_date,
                "cutoff_date": cutoff_date,
                "file_name": file_name,
                "rows": int(rows),
                "created_ts": created_ts,
            }
        ]
    )
    if registry_path.exists():
        registry = pd.read_csv(registry_path)
        registry = pd.concat([registry, new_row], ignore_index=True)
    else:
        registry = new_row
    registry = registry.drop_duplicates(subset=["snapshot_date", "file_name"], keep="last")
    registry = registry.sort_values(["snapshot_date", "file_name"]).reset_index(drop=True)
    registry.to_csv(registry_path, index=False)


def _update_supervisor_forecast_history(
    *,
    outputs_dir: Path,
    snapshot_date: str,
    supervisor_daily: pd.DataFrame,
    supervisor_weekly: pd.DataFrame,
) -> None:
    history_path = outputs_dir / "supervisor_forecast_history.csv"
    frames: list[pd.DataFrame] = []

    metric_map = {
        "salidas": "salidas_forecast",
        "recogidas": "recogidas_forecast",
        "pick_lines": "pick_lines_forecast",
    }

    if not supervisor_daily.empty:
        d = supervisor_daily.copy()
        for metric, col in metric_map.items():
            if col not in d.columns:
                continue
            part = pd.DataFrame(
                {
                    "snapshot_date": snapshot_date,
                    "grain": "daily",
                    "metric": metric,
                    "fecha": d["fecha"],
                    "week_iso": "",
                    "forecast_value": pd.to_numeric(d[col], errors="coerce").fillna(0.0),
                }
            )
            frames.append(part)

    if not supervisor_weekly.empty:
        w = supervisor_weekly.copy()
        for metric, col in metric_map.items():
            if col not in w.columns:
                continue
            part = pd.DataFrame(
                {
                    "snapshot_date": snapshot_date,
                    "grain": "weekly",
                    "metric": metric,
                    "fecha": "",
                    "week_iso": pd.to_numeric(w["week_iso"], errors="coerce"),
                    "forecast_value": pd.to_numeric(w[col], errors="coerce").fillna(0.0),
                }
            )
            frames.append(part)

    if not frames:
        return

    new_history = pd.concat(frames, ignore_index=True)
    if history_path.exists():
        history = pd.read_csv(history_path)
        history = pd.concat([history, new_history], ignore_index=True)
    else:
        history = new_history
    history = history.drop_duplicates(subset=["snapshot_date", "grain", "metric", "fecha", "week_iso"], keep="last")
    history = history.sort_values(["snapshot_date", "grain", "metric", "fecha", "week_iso"]).reset_index(drop=True)
    history.to_csv(history_path, index=False)


def _save_supervisor_snapshots(
    *,
    outputs_dir: Path,
    cutoff: pd.Timestamp,
    supervisor_daily: pd.DataFrame,
    supervisor_weekly: pd.DataFrame,
) -> None:
    snapshot_date = pd.Timestamp.today().normalize().strftime("%Y-%m-%d")
    created_ts = pd.Timestamp.utcnow().tz_localize(None).strftime("%Y-%m-%d %H:%M:%S")
    cutoff_date = pd.Timestamp(cutoff).normalize().strftime("%Y-%m-%d")

    snapshots_dir = outputs_dir / "history" / "supervisor_snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    files = [
        ("supervisor_dashboard_daily", supervisor_daily),
        ("supervisor_dashboard_weekly", supervisor_weekly),
    ]
    for base_name, frame in files:
        file_name = f"{base_name}__{snapshot_date}.csv"
        snapshot_path = snapshots_dir / file_name
        frame.to_csv(snapshot_path, index=False)
        _update_forecast_snapshot_registry(
            outputs_dir=outputs_dir,
            snapshot_date=snapshot_date,
            cutoff_date=cutoff_date,
            file_name=file_name,
            rows=len(frame),
            created_ts=created_ts,
        )

    _update_supervisor_forecast_history(
        outputs_dir=outputs_dir,
        snapshot_date=snapshot_date,
        supervisor_daily=supervisor_daily,
        supervisor_weekly=supervisor_weekly,
    )


def main() -> None:
    _setup_logging()
    args = parse_args()

    root = Path(".").resolve()
    outputs_dir = root / "outputs"
    processed_dir = root / "data" / "processed"
    diag_dir = outputs_dir / "diagnostics"
    models_dir = root / "models"
    outputs_dir.mkdir(exist_ok=True)
    processed_dir.mkdir(exist_ok=True, parents=True)
    diag_dir.mkdir(exist_ok=True, parents=True)
    models_dir.mkdir(exist_ok=True)

    cutoff = pd.Timestamp(args.cutoff_date).normalize() if args.cutoff_date else pd.Timestamp.today().normalize()
    operational_cutover = pd.Timestamp(args.operational_cutover_date).normalize()
    use_weather = parse_bool(args.use_weather)
    debug_join = parse_bool(args.debug_join)
    exclude_years = parse_exclude_years(args.exclude_years)

    LOGGER.info("Cutoff date: %s", cutoff.date())
    LOGGER.info("Data mode: %s", args.data_mode)
    LOGGER.info("Operational cutover date: %s", operational_cutover.date())
    LOGGER.info("Exclude years train/scoring: %s", exclude_years)

    paths = resolve_input_paths(root)
    raw_alb = load_albaranes(paths.albaranes)
    raw_mov = load_movimientos(paths.movimientos)
    raw_operational = (
        load_operational_orders(paths.operational_orders)
        if paths.operational_orders is not None
        else pd.DataFrame()
    )
    holidays_df = load_holidays(paths.holidays)
    prov_station_map = load_provincia_station_map(paths.provincia_station_map)

    albaranes = clean_albaranes(raw_alb, cutoff_date=cutoff)
    movimientos = clean_movimientos(raw_mov)

    if debug_join:
        generate_join_debug_outputs(albaranes=albaranes, movimientos=movimientos, outputs_dir=outputs_dir)
        LOGGER.info("Modo debug_join completado. Salida: %s", outputs_dir)
        return

    albaranes["provincia_norm"] = normalize_provincia_destino(albaranes["provincia_destino"], prov_station_map)
    canonical_layer = build_canonical_layer(
        albaranes_clean=albaranes,
        operational_raw=raw_operational,
        selection_mode=args.data_mode,
        cutoff=cutoff,
        cutover_date=operational_cutover,
    )
    canonical_layer.stg_services_legacy.to_parquet(processed_dir / "stg_services_legacy.parquet", index=False)
    canonical_layer.stg_services_operational.to_parquet(
        processed_dir / "stg_services_operational.parquet", index=False
    )
    canonical_layer.fact_services_canonical.to_parquet(
        processed_dir / "fact_services_canonical.parquet", index=False
    )

    service_level = canonical_layer.pipeline_services.copy()
    if service_level.empty:
        raise ValueError(
            "La capa canonica no genero servicios activos para el pipeline. "
            "Revisa fuentes legacy/operational y reglas de deduplicacion."
        )
    service_hist_for_join = service_level[service_level["fecha_servicio"] <= cutoff].copy()
    service_type_audit = build_service_type_audit(service_level)

    join_out = assign_movements_to_services(
        movements=movimientos,
        services=service_hist_for_join,
        window_days=args.assignment_window_days,
    )

    service_daily, service_weekly_initial = build_service_targets(service_level)
    workload_daily, workload_weekly_initial = build_workload_targets(join_out.movements_joined)
    service_daily = service_daily.drop(columns=["mix_entrega_vs_recogida"], errors="ignore")
    service_weekly_initial = service_weekly_initial.drop(columns=["mix_entrega_vs_recogida"], errors="ignore")

    service_daily = densify_daily_calendar(
        service_daily,
        target_cols=SERVICE_TARGETS,
        axis="service",
        cutoff_date=cutoff,
        exclude_years=exclude_years,
    )
    workload_daily = densify_daily_calendar(
        workload_daily,
        target_cols=WORKLOAD_TARGETS,
        axis="workload",
        cutoff_date=cutoff,
        exclude_years=exclude_years,
    )
    service_intensity_summary = build_service_intensity_summary(
        service_daily[(service_daily["date"] <= cutoff) & service_daily["date"].dt.year.isin(exclude_years).eq(False)].copy()
    )

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
    transport_daily = forecast_daily_business[
        [c for c in forecast_daily_business.columns if c == "fecha" or c.startswith(("eventos_", "m3_", "pales_", "cajas_", "peso_facturable_"))]
    ].copy()
    transport_weekly = forecast_weekly_business[
        [c for c in forecast_weekly_business.columns if c in {"week_iso", "year", "week_start_date", "week_end_date"} or c.startswith(("eventos_", "m3_", "pales_", "cajas_", "peso_facturable_"))]
    ].copy()
    workload_expected_daily = _post_process_forecasts(_build_workload_expected_daily(forecast_daily))
    workload_expected_weekly = _post_process_forecasts(_build_workload_expected_weekly(workload_expected_daily))
    supervisor_dashboard_daily = build_supervisor_dashboard_daily(
        forecast_daily_business=forecast_daily_business,
        service_daily=service_daily,
        workload_daily=workload_daily,
        canonical_services=canonical_layer.fact_services_canonical,
        cutoff=cutoff,
        cutover_date=operational_cutover,
    )
    supervisor_dashboard_weekly = build_supervisor_dashboard_weekly(supervisor_dashboard_daily)

    labor_standards = load_labor_standards(root / LABOR_STANDARDS_PATH)
    staffing_daily_plan = _post_process_forecasts(
        build_staffing_plan(
            forecast_daily_business.rename(columns={"fecha": "date"}),
            labor_standards,
            date_col="date",
        )
    )
    staffing_weekly_plan = _post_process_forecasts(
        build_staffing_plan(
            forecast_weekly_business.rename(columns={"week_start_date": "date"}),
            labor_standards,
            date_col="date",
        )
    )
    if not staffing_daily_plan.empty:
        staffing_daily_plan = staffing_daily_plan[
            pd.to_datetime(staffing_daily_plan["date"], errors="coerce").dt.normalize() > cutoff
        ].copy()
        staffing_daily_plan = staffing_daily_plan.rename(columns={"date": "fecha"})
    if not staffing_weekly_plan.empty:
        next_week_start = cutoff + pd.Timedelta(days=(7 - cutoff.dayofweek) % 7)
        if next_week_start <= cutoff:
            next_week_start = next_week_start + pd.Timedelta(days=7)
        staffing_weekly_plan = staffing_weekly_plan[
            pd.to_datetime(staffing_weekly_plan["date"], errors="coerce").dt.normalize() >= next_week_start
        ].copy()
        staffing_weekly_plan = staffing_weekly_plan.rename(columns={"date": "week_start_date"})
        staffing_weekly_plan = staffing_weekly_plan.merge(
            forecast_weekly_business[["week_iso", "year", "week_start_date", "week_end_date"]].drop_duplicates(),
            on="week_start_date",
            how="left",
        )

    model_health_summary = build_model_health_summary(
        backtest_df=backtest_df,
        model_registry_df=model_paths_df,
        join_kpis=join_out.join_kpis,
        service_type_audit=service_type_audit,
        latest_cutoff_date=cutoff,
        service_hist_latest=service_daily_hist["date"].max() if not service_daily_hist.empty else pd.NaT,
        workload_hist_latest=workload_daily_hist["date"].max() if not workload_daily_hist.empty else pd.NaT,
    )
    feature_policy_summary = build_feature_policy_summary()

    forecast_daily.to_csv(outputs_dir / "forecast_daily.csv", index=False)
    forecast_weekly.to_csv(outputs_dir / "forecast_weekly.csv", index=False)
    forecast_daily_business.to_csv(outputs_dir / "forecast_daily_business.csv", index=False)
    forecast_weekly_business.to_csv(outputs_dir / "forecast_weekly_business.csv", index=False)
    transport_daily.to_csv(outputs_dir / "transport_forecast_daily.csv", index=False)
    transport_weekly.to_csv(outputs_dir / "transport_forecast_weekly.csv", index=False)
    workload_expected_daily.to_csv(outputs_dir / "workload_expected_daily.csv", index=False)
    workload_expected_weekly.to_csv(outputs_dir / "workload_expected_weekly.csv", index=False)
    supervisor_dashboard_daily.to_csv(outputs_dir / "supervisor_dashboard_daily.csv", index=False)
    supervisor_dashboard_weekly.to_csv(outputs_dir / "supervisor_dashboard_weekly.csv", index=False)
    _save_supervisor_snapshots(
        outputs_dir=outputs_dir,
        cutoff=cutoff,
        supervisor_daily=supervisor_dashboard_daily,
        supervisor_weekly=supervisor_dashboard_weekly,
    )
    staffing_daily_plan.to_csv(outputs_dir / "staffing_daily_plan.csv", index=False)
    staffing_weekly_plan.to_csv(outputs_dir / "staffing_weekly_plan.csv", index=False)
    backtest_df.to_csv(outputs_dir / "backtest_metrics.csv", index=False)
    join_out.join_kpis.to_csv(outputs_dir / "join_kpis.csv", index=False)
    join_out.lead_time_summary.to_csv(outputs_dir / "lead_time_summary.csv", index=False)
    canonical_layer.source_audit.to_csv(outputs_dir / "pedidos_source_audit.csv", index=False)
    model_paths_df.to_csv(outputs_dir / "model_registry.csv", index=False)
    model_health_summary.to_csv(outputs_dir / "model_health_summary.csv", index=False)
    service_type_audit.to_csv(outputs_dir / "service_type_audit.csv", index=False)
    service_intensity_summary.to_csv(outputs_dir / "service_intensity_summary.csv", index=False)
    feature_policy_summary.to_csv(outputs_dir / "feature_policy.csv", index=False)

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
