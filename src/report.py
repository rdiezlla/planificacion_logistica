from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

LOGGER = logging.getLogger(__name__)


def _is_delivery_type(value: object) -> bool:
    if value is None:
        return False
    t = str(value).strip().lower()
    return t.startswith("entrega") or t == "mixto"


def _is_pickup_type(value: object) -> bool:
    if value is None:
        return False
    t = str(value).strip().lower()
    return t.startswith("recogida") or t == "mixto"


def save_backtest_plots(metrics_df: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if metrics_df.empty:
        LOGGER.warning("Sin metricas de backtest para graficar")
        return

    summary = (
        metrics_df.groupby(["axis", "freq", "target", "model"], dropna=False)[["wape", "smape", "mase", "empirical_coverage_p80", "pinball_loss_p50", "pinball_loss_p80"]]
        .mean()
        .reset_index()
    )

    for (axis, freq), g in summary.groupby(["axis", "freq"], dropna=False):
        fig, ax = plt.subplots(figsize=(12, 6))
        pivot = g.pivot_table(index="target", columns="model", values="wape", aggfunc="mean")
        pivot.plot(kind="bar", ax=ax)
        ax.set_title(f"Backtest WAPE - {axis} {freq}")
        ax.set_ylabel("WAPE")
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        p = out_dir / f"backtest_wape_{axis}_{freq}.png"
        fig.savefig(p, dpi=140)
        plt.close(fig)

        if "empirical_coverage_p80" in g.columns and g["empirical_coverage_p80"].notna().any():
            fig_cov, ax_cov = plt.subplots(figsize=(12, 6))
            pivot_cov = g.pivot_table(index="target", columns="model", values="empirical_coverage_p80", aggfunc="mean")
            pivot_cov.plot(kind="bar", ax=ax_cov)
            ax_cov.axhline(0.8, linestyle="--", color="#10b981", linewidth=1)
            ax_cov.set_title(f"Backtest empirical coverage P80 - {axis} {freq}")
            ax_cov.set_ylabel("Coverage")
            ax_cov.grid(axis="y", alpha=0.3)
            fig_cov.tight_layout()
            fig_cov.savefig(out_dir / f"backtest_coverage_p80_{axis}_{freq}.png", dpi=140)
            plt.close(fig_cov)


def _plot_series(series: pd.DataFrame, title: str, out_path: Path, ylabel: str = "Prediccion") -> None:
    if series.empty:
        return
    fig, ax = plt.subplots(figsize=(12, 6))
    for c in series.columns:
        ax.plot(series.index, series[c], label=c)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.grid(alpha=0.3)
    ax.legend(loc="best", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def save_forecast_plots(forecast_df: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if forecast_df.empty:
        return

    axis_plot_cols = {
        "workload": [
            "conteo_movimientos_p50",
            "picking_movs_p50",
            "workload_atribuible_movs_p50",
            "workload_no_atribuible_movs_p50",
        ],
        "workload_expected_from_service": [
            "picking_movs_esperados_desde_servicio_p50",
            "picking_movs_esperados_desde_servicio_p80",
            "inbound_recepcion_pales_esperados_p50",
            "inbound_ubicacion_cajas_esperados_p50",
            "inbound_m3_esperados_p50",
        ],
    }

    for axis, g_axis in forecast_df.groupby("axis", dropna=False):
        if str(axis) == "service":
            g = g_axis.copy()
            g["date"] = pd.to_datetime(g["date"], errors="coerce").dt.normalize()

            out_rows = g[g["tipo_servicio"].map(_is_delivery_type)].copy()
            out_cols = [c for c in ["conteo_servicios_p50", "m3_out_p50", "cajas_out_p50", "pales_out_p50"] if c in out_rows.columns]
            if out_cols:
                s_out = out_rows.groupby("date", dropna=False)[out_cols].sum(min_count=1).sort_index()
                s_out = s_out.rename(columns={"conteo_servicios_p50": "eventos_entrega_p50"}).dropna(axis=1, how="all")
                _plot_series(s_out, "Forecast P50 - service OUT", out_dir / "forecast_p50_service_out.png")

            in_rows = g[g["tipo_servicio"].map(_is_pickup_type)].copy()
            in_cols = [c for c in ["conteo_servicios_p50", "m3_in_p50", "cajas_in_p50", "pales_in_p50"] if c in in_rows.columns]
            if in_cols:
                s_in = in_rows.groupby("date", dropna=False)[in_cols].sum(min_count=1).sort_index()
                s_in = s_in.rename(columns={"conteo_servicios_p50": "eventos_recogida_p50"}).dropna(axis=1, how="all")
                _plot_series(s_in, "Forecast P50 - service IN", out_dir / "forecast_p50_service_in.png")

            # Grafico resumen legacy para compatibilidad.
            value_cols = [c for c in ["conteo_servicios_p50", "m3_out_p50", "cajas_out_p50", "pales_out_p50"] if c in g.columns]
            if value_cols:
                s_all = g.groupby("date", dropna=False)[value_cols].sum(min_count=1).sort_index().dropna(axis=1, how="all")
                _plot_series(s_all, "Forecast P50 - service", out_dir / "forecast_p50_service.png")
            continue

        wanted = axis_plot_cols.get(str(axis), [c for c in g_axis.columns if c.endswith("_p50")])
        selected = [c for c in wanted if c in g_axis.columns]
        if not selected:
            continue

        series = (
            g_axis.groupby("date", dropna=False)[selected]
            .sum(min_count=1)
            .sort_index()
        )
        series = series.dropna(axis=1, how="all")
        if series.empty:
            continue

        _plot_series(series, f"Forecast P50 - {axis}", out_dir / f"forecast_p50_{axis}.png")


def save_picking_validation_plot(
    expected_hist_daily: pd.DataFrame,
    workload_daily_hist: pd.DataFrame,
    out_dir: Path,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if expected_hist_daily.empty or workload_daily_hist.empty:
        return

    exp = expected_hist_daily.copy()
    exp["date"] = pd.to_datetime(exp["date"], errors="coerce").dt.normalize()
    if "picking_movs_esperados_desde_servicio_p50" not in exp.columns:
        return

    real = workload_daily_hist.copy()
    real["date"] = pd.to_datetime(real["date"], errors="coerce").dt.normalize()
    real_col = "picking_movs_atribuibles_entrega" if "picking_movs_atribuibles_entrega" in real.columns else "picking_movs_atribuibles"
    if real_col not in real.columns:
        return

    merged = real[["date", real_col]].merge(
        exp[["date", "picking_movs_esperados_desde_servicio_p50", "picking_movs_esperados_desde_servicio_p80"]],
        on="date",
        how="inner",
    )
    if merged.empty:
        return

    series = merged.set_index("date")[[real_col, "picking_movs_esperados_desde_servicio_p50", "picking_movs_esperados_desde_servicio_p80"]].copy()
    series = series.rename(columns={real_col: "picking_real_entrega"})
    _plot_series(
        series,
        "Validacion historica - picking esperado vs real entrega",
        out_dir / "picking_expected_vs_real_entrega.png",
        ylabel="Movimientos PI",
    )
