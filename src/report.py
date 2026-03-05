from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

LOGGER = logging.getLogger(__name__)


def save_backtest_plots(metrics_df: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if metrics_df.empty:
        LOGGER.warning("Sin metricas de backtest para graficar")
        return

    summary = (
        metrics_df.groupby(["axis", "freq", "target", "model"], dropna=False)[["wape", "smape", "mase"]]
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


def save_forecast_plots(forecast_df: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if forecast_df.empty:
        return

    for axis, g_axis in forecast_df.groupby("axis", dropna=False):
        value_cols = [c for c in g_axis.columns if c.endswith("_p50")]
        if not value_cols:
            continue
        top_cols = value_cols[:4]

        fig, ax = plt.subplots(figsize=(12, 6))
        series = g_axis.groupby("date", dropna=False)[top_cols].sum().sort_index()
        for c in top_cols:
            ax.plot(series.index, series[c], label=c)

        ax.set_title(f"Forecast P50 - {axis}")
        ax.set_ylabel("Prediccion")
        ax.grid(alpha=0.3)
        ax.legend(loc="best", fontsize=8)
        fig.tight_layout()
        p = out_dir / f"forecast_p50_{axis}.png"
        fig.savefig(p, dpi=140)
        plt.close(fig)
