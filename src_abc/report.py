from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)

CLASS_COLORS = {"A": "#0f766e", "B": "#f59e0b", "C": "#94a3b8"}


def _md_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "_sin datos_"
    safe = df.fillna("")
    cols = [str(col) for col in safe.columns]
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for _, row in safe.iterrows():
        values = [str(row[col]).replace("\n", " ").replace("|", "/") for col in safe.columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def generate_abc_plots(
    output_dir: Path,
    annual_df: pd.DataFrame,
    quarterly_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    top_changes_df: pd.DataFrame,
    layout_df: pd.DataFrame,
    top_n: int = 40,
) -> None:
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    for year, subset in annual_df.groupby("year", dropna=False):
        _plot_pareto(
            subset.sort_values("rank_in_period").head(top_n),
            plots_dir / f"pareto_annual_{int(year)}.png",
            title=f"Pareto ABC picking anual {int(year)}",
        )

    for period_label, subset in quarterly_df.groupby("period_label", dropna=False):
        safe_label = str(period_label).replace("-", "_")
        _plot_pareto(
            subset.sort_values("rank_in_period").head(top_n),
            plots_dir / f"pareto_quarterly_{safe_label}.png",
            title=f"Pareto ABC picking {period_label}",
        )

    _plot_year_comparison(summary_df, plots_dir / "abc_year_comparison.png")
    _plot_class_change_table(top_changes_df, plots_dir / "abc_changes_heatmap.png")
    _plot_latest_a_skus(layout_df, plots_dir / "abc_top_a_latest.png")


def write_readme_abc(path: Path, stats, summary_df: pd.DataFrame, layout_df: pd.DataFrame, top_changes_df: pd.DataFrame) -> None:
    latest_summary = summary_df.sort_values("period_end_date").tail(1)
    latest_label = latest_summary["period_label"].iloc[0] if not latest_summary.empty else "-"
    latest_a_share = latest_summary["pct_pick_lines_A"].iloc[0] if not latest_summary.empty else 0.0
    top_layout = layout_df.head(15)
    biggest_moves = top_changes_df.loc[top_changes_df["movement_direction"].ne("stable")].head(20)

    lines = [
        "# ABC Picking",
        "",
        "Analisis Pareto / clasificacion ABC de picking basado en `pick_lines` (numero de movimientos PI por SKU).",
        "",
        "## Regla principal",
        "",
        "- La clasificacion ABC se calcula por `pick_lines`, no por unidades.",
        "- Las metricas secundarias (`pick_qty`, `n_orders`, `n_days_active`) son auxiliares para layout y slotting.",
        "",
        "## Cobertura del analisis",
        "",
        f"- Lineas PI validas con fecha: {stats.n_rows_after_date_filter}",
        f"- SKUs analizados: {stats.n_unique_skus}",
        f"- Rango de fechas: {stats.min_pick_date} -> {stats.max_pick_date}",
        f"- Registros descartados por fecha invalida: {stats.n_rows_dropped_invalid_date}",
        "",
        "## Como usarlo para layout",
        "",
        "- `A`: SKUs de mayor concentracion operativa. Son candidatos a posiciones frontales o zonas calientes.",
        "- `B`: mantener accesibles, pero sin consumir ubicaciones premium si no hay restriccion operativa.",
        "- `C`: baja prioridad; revisar si ocupan espacio prime de forma innecesaria.",
        "",
        f"Ultimo periodo disponible: `{latest_label}`",
        f"Concentracion de pick_lines en clase A: `{latest_a_share:.1%}`",
        "",
        "## Candidatos de layout (ultimo periodo)",
        "",
        _md_table(top_layout),
        "",
        "## Cambios relevantes entre periodos",
        "",
        _md_table(biggest_moves),
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    LOGGER.info("README ABC generado: %s", path)


def _plot_pareto(df: pd.DataFrame, path: Path, title: str) -> None:
    if df.empty:
        return
    fig, ax1 = plt.subplots(figsize=(13, 6))
    x = np.arange(len(df))
    colors = [CLASS_COLORS.get(str(v), "#94a3b8") for v in df["abc_class"]]
    ax1.bar(x, df["pick_lines"], color=colors, width=0.85)
    ax1.set_title(title)
    ax1.set_ylabel("pick_lines")
    ax1.set_xlabel("SKU")
    ax1.set_xticks(x)
    ax1.set_xticklabels(df["sku"].astype(str), rotation=90, fontsize=8)

    ax2 = ax1.twinx()
    ax2.plot(x, df["cumulative_pct"] * 100.0, color="#1e293b", linewidth=2.3)
    ax2.axhline(80, color="#0f766e", linestyle="--", linewidth=1)
    ax2.axhline(95, color="#f59e0b", linestyle="--", linewidth=1)
    ax2.set_ylabel("Cumulative %")
    ax2.set_ylim(0, 105)
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)


def _plot_year_comparison(summary_df: pd.DataFrame, path: Path) -> None:
    annual_summary = summary_df.loc[summary_df["period_type"].eq("annual")].copy()
    if annual_summary.empty:
        return
    annual_summary = annual_summary.sort_values("year")
    fig, ax = plt.subplots(figsize=(10, 5))
    years = annual_summary["year"].astype(str)
    ax.bar(years, annual_summary["pct_pick_lines_A"] * 100.0, label="A", color=CLASS_COLORS["A"])
    ax.bar(
        years,
        annual_summary["pct_pick_lines_B"] * 100.0,
        bottom=annual_summary["pct_pick_lines_A"] * 100.0,
        label="B",
        color=CLASS_COLORS["B"],
    )
    ax.bar(
        years,
        annual_summary["pct_pick_lines_C"] * 100.0,
        bottom=(annual_summary["pct_pick_lines_A"] + annual_summary["pct_pick_lines_B"]) * 100.0,
        label="C",
        color=CLASS_COLORS["C"],
    )
    ax.set_title("Comparativa ABC por ano")
    ax.set_ylabel("% pick_lines")
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)


def _plot_class_change_table(top_changes_df: pd.DataFrame, path: Path, top_n: int = 20) -> None:
    changes = top_changes_df.loc[top_changes_df["movement_direction"].ne("stable")].copy()
    if changes.empty:
        return
    sku_rank = (
        changes.groupby("sku", dropna=False)["rank_delta"]
        .apply(lambda s: s.abs().sum())
        .sort_values(ascending=False)
        .head(top_n)
    )
    pivot = (
        changes.loc[changes["sku"].isin(sku_rank.index)]
        .pivot_table(index="sku", columns="curr_period", values="curr_abc_class", aggfunc="first")
        .fillna("")
    )
    if pivot.empty:
        return

    color_map = {"A": "#d1fae5", "B": "#fef3c7", "C": "#e2e8f0", "": "#ffffff"}
    fig, ax = plt.subplots(figsize=(max(8, pivot.shape[1] * 1.3), max(4, pivot.shape[0] * 0.45)))
    ax.axis("off")
    table = ax.table(
        cellText=pivot.values,
        rowLabels=pivot.index.tolist(),
        colLabels=pivot.columns.tolist(),
        cellLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.4)
    for (row, col), cell in table.get_celld().items():
        if row == 0 or col == -1:
            cell.set_facecolor("#e2e8f0")
            continue
        value = pivot.iloc[row - 1, col]
        cell.set_facecolor(color_map.get(str(value), "#ffffff"))
    ax.set_title("Cambios de clase ABC por SKU")
    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def _plot_latest_a_skus(layout_df: pd.DataFrame, path: Path, top_n: int = 20) -> None:
    latest_a = layout_df.loc[layout_df["latest_abc_class"].eq("A")].head(top_n).copy()
    if latest_a.empty:
        return
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.barh(latest_a["sku"].astype(str), latest_a["latest_pick_lines"], color=CLASS_COLORS["A"])
    ax.invert_yaxis()
    ax.set_title("Top SKUs A del ultimo periodo")
    ax.set_xlabel("pick_lines")
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)
