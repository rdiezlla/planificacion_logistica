from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)


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


def generate_plots(
    outputs_dir: Path,
    summary_oper: pd.DataFrame,
    summary_order: pd.DataFrame,
    pairs_oper: pd.DataFrame,
    pairs_order: pd.DataFrame,
    sku_freq_oper: pd.DataFrame,
    sku_freq_order: pd.DataFrame,
) -> None:
    plots_dir = outputs_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    for level, summary in [("oper", summary_oper), ("order", summary_order)]:
        fig, ax = plt.subplots(figsize=(10, 5))
        summary["n_skus"].clip(upper=40).plot.hist(
            bins=30,
            ax=ax,
            color="#5f90ff" if level == "order" else "#eb6a9b",
            alpha=0.85,
        )
        ax.set_title(f"Distribucion de tamano de cesta - {level}")
        ax.set_xlabel("SKUs distintos por transaccion")
        ax.set_ylabel("Frecuencia")
        fig.tight_layout()
        fig.savefig(plots_dir / f"basket_size_hist_{level}.png", dpi=160)
        plt.close(fig)

    _plot_heatmap(plots_dir / "heatmap_top_skus_oper.png", pairs_oper, sku_freq_oper, "oper")
    _plot_heatmap(plots_dir / "heatmap_top_skus_order.png", pairs_order, sku_freq_order, "order")


def _plot_heatmap(path: Path, pairs: pd.DataFrame, sku_freq: pd.DataFrame, level: str) -> None:
    if pairs.empty or sku_freq.empty:
        return
    if level == "oper":
        freq = (
            sku_freq.groupby("sku", dropna=False)["transaction_count"]
            .sum()
            .sort_values(ascending=False)
            .head(15)
        )
        pair_source = (
            pairs.groupby(["sku_a", "sku_b"], dropna=False)["count_pair"]
            .sum()
            .reset_index()
        )
    else:
        freq = (
            sku_freq.loc[sku_freq["segment"].eq("ALL")]
            .sort_values("transaction_count", ascending=False)
            .head(15)
            .set_index("sku")["transaction_count"]
        )
        pair_source = pairs.loc[pairs["segment"].eq("ALL"), ["sku_a", "sku_b", "count_pair"]].copy()

    top_skus = freq.index.tolist()
    matrix = pd.DataFrame(0.0, index=top_skus, columns=top_skus)
    for _, rec in pair_source.iterrows():
        sku_a = str(rec["sku_a"])
        sku_b = str(rec["sku_b"])
        if sku_a in matrix.index and sku_b in matrix.columns:
            matrix.loc[sku_a, sku_b] += float(rec["count_pair"])
            matrix.loc[sku_b, sku_a] += float(rec["count_pair"])

    fig, ax = plt.subplots(figsize=(10, 8))
    im = ax.imshow(matrix.values, cmap="magma")
    ax.set_xticks(np.arange(len(top_skus)))
    ax.set_xticklabels(top_skus, rotation=90, fontsize=8)
    ax.set_yticks(np.arange(len(top_skus)))
    ax.set_yticklabels(top_skus, fontsize=8)
    ax.set_title(f"Heatmap top co-ocurrencias SKU - {level}")
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)


def estimate_location_savings(lines: pd.DataFrame, pairs_oper: pd.DataFrame) -> pd.DataFrame:
    if "ubicacion_norm" not in lines.columns or lines["ubicacion_norm"].eq("").all():
        return pd.DataFrame()

    location_map = (
        lines.loc[lines["ubicacion_norm"].ne("")]
        .groupby(["propietario_norm", "sku"], dropna=False)["ubicacion_norm"]
        .agg(lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0])
        .reset_index()
    )
    lookup = location_map.set_index(["propietario_norm", "sku"])["ubicacion_norm"]

    rows: list[dict[str, object]] = []
    for _, rec in pairs_oper.iterrows():
        segment = str(rec["segment"])
        key_a = (segment, str(rec["sku_a"]))
        key_b = (segment, str(rec["sku_b"]))
        if key_a not in lookup.index or key_b not in lookup.index:
            continue
        loc_a = str(lookup.loc[key_a])
        loc_b = str(lookup.loc[key_b])
        aisle_a = _extract_numeric_prefix(loc_a)
        aisle_b = _extract_numeric_prefix(loc_b)
        if aisle_a is None or aisle_b is None:
            continue
        aisle_gap = abs(aisle_a - aisle_b)
        if aisle_gap <= 0:
            continue
        rows.append(
            {
                "segment": segment,
                "sku_a": rec["sku_a"],
                "sku_b": rec["sku_b"],
                "ubicacion_a": loc_a,
                "ubicacion_b": loc_b,
                "aisle_gap": aisle_gap,
                "count_pair": rec["count_pair"],
                "savings_index": aisle_gap * float(rec["count_pair"]),
            }
        )
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["savings_index", "count_pair"], ascending=[False, False]).reset_index(drop=True)
    return out


def _extract_numeric_prefix(location: str) -> int | None:
    if not location:
        return None
    prefix = location.split("-")[0].strip()
    if prefix.isdigit():
        return int(prefix)
    return None


def write_readme_basket(
    path: Path,
    load_stats,
    views: dict[str, object],
    pairs_oper: pd.DataFrame,
    pairs_order: pd.DataFrame,
    triples_oper: pd.DataFrame,
    triples_order: pd.DataFrame,
    clusters_oper: pd.DataFrame,
    penalty_kpis: pd.DataFrame,
    location_savings: pd.DataFrame,
) -> None:
    del pairs_order
    del triples_order
    del clusters_oper
    del penalty_kpis
    del location_savings

    lines = [
        "# README_BASKET",
        "",
        "## Objetivo del modulo",
        "",
        "`basket_main.py` genera analitica de co-ocurrencia para optimizacion de picking y layout.",
        "",
        "Regla de negocio central:",
        "",
        "- Para layout, priorizar nivel operativo `pedido x propietario` (`oper`).",
        "- El nivel `order` sirve como contraste.",
        "- El modulo escribe resultados en `outputs_basket/`.",
        "",
        "## Inputs",
        "",
        "- Por defecto se usa `movimientos.xlsx` resuelto automaticamente desde OneDrive `Descargas BI`.",
        "- El flujo crudo asociado vive en `Descargas BI/Movimientos/` y `Descargas BI/Movimientos pedidos/`.",
        "",
        "## Ejecucion",
        "",
        "### Mac/Linux",
        "",
        "```bash",
        "source .venv/bin/activate",
        "python basket_main.py --output_dir outputs_basket",
        "```",
        "",
        "### Windows PowerShell",
        "",
        "```powershell",
        "Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass",
        ".venv\\Scripts\\activate",
        "python basket_main.py --output_dir outputs_basket",
        "```",
        "",
        "## Outputs principales",
        "",
        "- `transactions_summary_oper.csv`",
        "- `transactions_summary_order.csv`",
        "- `sku_frequency_oper.csv`",
        "- `sku_frequency_order.csv`",
        "- `top_pairs_oper.csv`",
        "- `top_pairs_order.csv`",
        "- `rules_oper.csv`",
        "- `rules_order.csv`",
        "- `top_triples_oper.csv`",
        "- `top_triples_order.csv`",
        "- `sku_clusters_oper.csv`",
        "- `sku_clusters_order.csv`",
        "- `order_owner_penalty.csv`",
        "- `owner_penalty_kpis.csv`",
        "- `location_savings_oper.csv` (si hay ubicaciones validas)",
        "- `plots/*.png`",
        "",
        "## Cobertura del ultimo run",
        "",
        f"- Filas raw movimientos: {load_stats.n_rows_raw}",
        f"- Filas PI: {load_stats.n_rows_pi}",
        f"- Filas PI validas: {load_stats.n_rows_valid}",
        f"- % propietario desconocido: {load_stats.pct_missing_owner:.2%}",
        f"- % missing ubicacion: {load_stats.pct_missing_ubicacion:.2%}",
        f"- Filas en `top_pairs_oper.csv`: {len(pairs_oper)}",
        f"- Filas en `top_triples_oper.csv`: {len(triples_oper)}",
        f"- Transacciones oper: {len(views['oper'].summary_all)}",
        f"- Transacciones order: {len(views['order'].summary_all)}",
        "",
        "## Conexion con el resto del proyecto",
        "",
        "- No recalcula el forecast principal de `main.py`.",
        "- Streamlit (pagina `Optimizacion picking`) consume `outputs_basket/`.",
        "- La web React actual no consume `outputs_basket/`.",
        "",
    ]

    path.write_text("\n".join(lines), encoding="utf-8")
    LOGGER.info("README basket generado: %s", path)
