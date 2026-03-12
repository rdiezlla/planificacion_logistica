from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)

CLASS_COLORS = {"A": "#0f766e", "B": "#f59e0b", "C": "#94a3b8"}
XYZ_COLORS = {"X": "#0f766e", "Y": "#f59e0b", "Z": "#ef4444", "LOW_HISTORY": "#6366f1", "UNKNOWN": "#94a3b8"}


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

    annual_global = _global_view(annual_df)
    quarterly_global = _global_view(quarterly_df)
    summary_global = _global_view(summary_df)
    changes_global = _global_view(top_changes_df)
    layout_global = _global_view(layout_df)

    for year, subset in annual_global.groupby("year", dropna=False):
        _plot_pareto(
            subset.sort_values("rank_in_period").head(top_n),
            plots_dir / f"pareto_annual_{int(year)}.png",
            title=f"Pareto ABC picking anual {int(year)}",
        )

    for period_label, subset in quarterly_global.groupby("period_label", dropna=False):
        safe_label = str(period_label).replace("-", "_")
        _plot_pareto(
            subset.sort_values("rank_in_period").head(top_n),
            plots_dir / f"pareto_quarterly_{safe_label}.png",
            title=f"Pareto ABC picking {period_label}",
        )

    _plot_year_comparison(summary_global, plots_dir / "abc_year_comparison.png")
    _plot_class_change_table(changes_global, plots_dir / "abc_changes_heatmap.png")
    _plot_latest_a_skus(layout_global, plots_dir / "abc_top_a_latest.png")
    _plot_latest_xyz_scatter(layout_global, plots_dir / "abc_xyz_scatter_latest.png")


def write_readme_abc(
    path: Path,
    stats,
    summary_df: pd.DataFrame,
    abc_xyz_summary_df: pd.DataFrame,
    owner_summary_df: pd.DataFrame,
    layout_df: pd.DataFrame,
    top_changes_df: pd.DataFrame,
) -> None:
    del abc_xyz_summary_df
    del owner_summary_df
    del layout_df
    del top_changes_df

    summary_global = _global_view(summary_df)
    latest_summary = pd.DataFrame()
    if not summary_global.empty:
        if "period_end_date" in summary_global.columns:
            latest_summary = summary_global.sort_values("period_end_date").tail(1)
        else:
            latest_summary = summary_global.tail(1)

    latest_label = (
        str(latest_summary["period_label"].iloc[0])
        if (not latest_summary.empty and "period_label" in latest_summary.columns)
        else "-"
    )
    latest_a_share = (
        float(latest_summary["pct_pick_lines_A"].iloc[0])
        if (not latest_summary.empty and "pct_pick_lines_A" in latest_summary.columns)
        else float("nan")
    )

    lines = [
        "# README_ABC",
        "",
        "## Objetivo del modulo",
        "",
        "`abc_main.py` genera clasificacion ABC-XYZ de picking para priorizar SKU y apoyar decisiones de layout.",
        "",
        "- `ABC`: importancia por `pick_lines` (no por unidades).",
        "- `XYZ`: estabilidad/variabilidad semanal de `pick_lines`.",
        "- El modulo escribe resultados en `outputs_abc/`.",
        "",
        "## Inputs",
        "",
        "- `movimientos.xlsx`",
        "",
        "## Ejecucion",
        "",
        "### Mac/Linux",
        "",
        "```bash",
        "source .venv/bin/activate",
        "python abc_main.py --input movimientos.xlsx --output_dir outputs_abc",
        "```",
        "",
        "### Windows PowerShell",
        "",
        "```powershell",
        "Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass",
        ".venv\\Scripts\\activate",
        "python abc_main.py --input movimientos.xlsx --output_dir outputs_abc",
        "```",
        "",
        "## Outputs principales",
        "",
        "- `abc_picking_annual.csv`",
        "- `abc_picking_quarterly.csv`",
        "- `abc_picking_ytd.csv`",
        "- `abc_summary_by_period.csv`",
        "- `abc_xyz_summary_by_period.csv`",
        "- `abc_owner_summary.csv`",
        "- `abc_top_changes.csv`",
        "- `abc_for_layout_candidates.csv`",
        "- `plots/*.png`",
        "",
        "## Cobertura del ultimo run",
        "",
        f"- Lineas PI validas con fecha: {stats.n_rows_after_date_filter}",
        f"- SKUs analizados: {stats.n_unique_skus}",
        f"- Owners analizados: {stats.n_unique_owners}",
        f"- Rango de fechas: {stats.min_pick_date} -> {stats.max_pick_date}",
        f"- Registros descartados por fecha invalida: {stats.n_rows_dropped_invalid_date}",
        f"- Ultimo periodo global disponible: {latest_label}",
        (
            f"- Concentracion de pick_lines en clase A (ultimo periodo): {latest_a_share:.1%}"
            if not np.isnan(latest_a_share)
            else "- Concentracion de pick_lines en clase A (ultimo periodo): n/d"
        ),
        "",
        "## Conexion con el resto del proyecto",
        "",
        "- No recalcula el forecast principal de `main.py`.",
        "- Streamlit (pagina `ABC Picking`) consume `outputs_abc/` para visualizacion.",
        "- La web React actual no consume `outputs_abc/`.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    LOGGER.info("README ABC generado: %s", path)

def _global_view(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if "owner_scope" not in df.columns:
        return df.copy()
    global_df = df.loc[df["owner_scope"].astype(str).eq("GLOBAL")].copy()
    return global_df if not global_df.empty else df.copy()


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


def _plot_latest_xyz_scatter(layout_df: pd.DataFrame, path: Path, top_n: int = 150) -> None:
    latest = layout_df.head(top_n).copy()
    if latest.empty:
        return
    latest["xyz_color"] = latest["xyz_class"].map(XYZ_COLORS).fillna("#94a3b8")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(
        latest["mean_weekly_pick_lines"],
        latest["cv_weekly"].fillna(0),
        s=np.clip(latest["latest_pick_lines"].fillna(0) * 1.2, 20, 350),
        c=latest["xyz_color"],
        alpha=0.7,
    )
    ax.axhline(0.5, linestyle="--", linewidth=1, color="#0f766e")
    ax.axhline(1.0, linestyle="--", linewidth=1, color="#f59e0b")
    ax.set_title("ABC-XYZ scatter del ultimo periodo (GLOBAL)")
    ax.set_xlabel("mean_weekly_pick_lines")
    ax.set_ylabel("cv_weekly")
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)
