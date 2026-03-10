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
    oper_agg = (
        pairs_oper.groupby(["sku_a", "sku_b"], dropna=False)
        .agg(count_pair=("count_pair", "sum"), lift=("lift", "max"))
        .reset_index()
        .sort_values(["count_pair", "lift"], ascending=[False, False])
    )
    oper_agg_sig = oper_agg.loc[oper_agg["count_pair"].ge(5) & oper_agg["lift"].ge(1.2)].copy()
    order_pairs = pairs_order.loc[pairs_order["segment"].eq("ALL")].copy()
    order_pairs_sig = order_pairs.loc[order_pairs["count_pair"].ge(5) & order_pairs["lift"].ge(1.2)].copy()

    oper_pair_set = {tuple(sorted((str(r["sku_a"]), str(r["sku_b"])))) for _, r in oper_agg_sig.iterrows()}
    order_pair_set = {tuple(sorted((str(r["sku_a"]), str(r["sku_b"])))) for _, r in order_pairs_sig.iterrows()}
    order_only = order_pairs_sig[
        order_pairs_sig.apply(lambda r: tuple(sorted((str(r["sku_a"]), str(r["sku_b"])))) not in oper_pair_set, axis=1)
    ].head(10)
    oper_only = oper_agg_sig[
        oper_agg_sig.apply(lambda r: tuple(sorted((str(r["sku_a"]), str(r["sku_b"])))) not in order_pair_set, axis=1)
    ].head(10)

    top_oper_pairs = pairs_oper.loc[pairs_oper["count_pair"].ge(5) & pairs_oper["lift"].ge(1.2)].head(20)
    top_oper_triples = triples_oper.loc[triples_oper["count_triple"].ge(5)].head(20)
    cluster_summary = (
        clusters_oper.groupby(["segment", "cluster_id"], dropna=False)
        .agg(
            cluster_size=("sku", "size"),
            cluster_transaction_count=("transaction_count", "sum"),
            sku_examples=("sku", lambda s: ", ".join(s.astype(str).head(5).tolist())),
        )
        .reset_index()
        .sort_values(["cluster_transaction_count", "cluster_size"], ascending=[False, False])
        .head(20)
    )

    lines = [
        "# Basket analysis para layout de picking",
        "",
        "## Regla de negocio clave",
        "",
        "Para layout y productividad hay que usar SIEMPRE el nivel operativo `pedido x propietario`.",
        "La PDA separa el recorrido por propietario, asi que un mismo pedido con 2 propietarios implica 2 rutas.",
        "",
        "## Calidad y limpieza",
        "",
        f"- Filas raw movimientos: {load_stats.n_rows_raw}",
        f"- Filas PI: {load_stats.n_rows_pi}",
        f"- Filas PI validas: {load_stats.n_rows_valid}",
        f"- % propietario DESCONOCIDO: {load_stats.pct_missing_owner:.2%}",
        f"- % missing ubicacion: {load_stats.pct_missing_ubicacion:.2%}",
        "- Transacciones con <2 SKUs se excluyen del basket porque no aportan co-ocurrencia.",
        "- Transacciones > max_basket_size se tratan como outliers operativos: suelen ser olas, consolidaciones o picks atipicos y sesgan pares/trios.",
        "",
        "## Impacto multi-propietario",
        "",
        _md_table(penalty_kpis),
        "",
        "## Recomendacion principal de layout",
        "",
        "Usar el analisis `oper` para reslotting dentro de cada propietario. El nivel `order` solo sirve como contraste y puede inducir co-ocurrencias falsas al mezclar propietarios.",
        "",
        "## Top 20 pares a co-localizar (oper)",
        "",
        _md_table(top_oper_pairs[["segment", "sku_a", "sku_b", "count_pair", "lift"]]),
        "",
        "## Top 20 trios a revisar juntos (oper)",
        "",
        _md_table(
            top_oper_triples[["segment", "sku_1", "sku_2", "sku_3", "count_triple"]]
            if not top_oper_triples.empty
            else pd.DataFrame({"info": ["Sin trios frecuentes con el soporte configurado"]})
        ),
        "",
        "## Top clusters / familias de picking (oper)",
        "",
        _md_table(
            cluster_summary
            if not cluster_summary.empty
            else pd.DataFrame({"info": ["Sin clusters suficientes"]})
        ),
        "",
        "## Comparativa oper vs order",
        "",
        "### Pares que aparecen en order pero no en oper",
        "",
        _md_table(
            order_only[["sku_a", "sku_b", "count_pair", "lift"]]
            if not order_only.empty
            else pd.DataFrame({"info": ["Ninguno"]})
        ),
        "",
        "### Pares que aparecen en oper pero no en order",
        "",
        _md_table(
            oper_only[["sku_a", "sku_b", "count_pair", "lift"]]
            if not oper_only.empty
            else pd.DataFrame({"info": ["Ninguno"]})
        ),
        "",
        "## Propuesta de accion",
        "",
        "- Re-slotting por propietario: agrupar primero pares y trios recurrentes del nivel oper.",
        "- Crear familias de picking usando clusters oper para reducir cambios de pasillo y rebusca.",
        "- No tomar decisiones de layout usando solo el nivel pedido porque mezcla propietarios y sobreestima afinidades.",
        "",
    ]

    if not location_savings.empty:
        lines.extend(
            [
                "## Heuristica de ahorro con ubicacion",
                "",
                "Proxy sencillo: `savings_index = aisle_gap x count_pair` usando el primer bloque numerico de ubicacion.",
                "",
                _md_table(location_savings.head(20)),
                "",
            ]
        )

    lines.extend(
        [
            "## Experimento recomendado",
            "",
            "1. Seleccionar 1-2 propietarios con mayor volumen y mayor `savings_index`.",
            "2. Reubicar 10-20 SKUs del top de pares/trios oper.",
            "3. Medir antes y despues: TPH, tiempo por linea, metros o pasillos recorridos y productividad por turno.",
            "4. Revisar el efecto separado en pedidos mono-propietario y multi-propietario.",
            "",
            "## Archivos de salida",
            "",
            f"- `transactions_summary_oper.csv`: {len(views['oper'].summary_all)} transacciones oper",
            f"- `transactions_summary_order.csv`: {len(views['order'].summary_all)} transacciones order",
            f"- `top_pairs_oper.csv`: {len(pairs_oper)} filas",
            f"- `top_pairs_order.csv`: {len(pairs_order)} filas",
            f"- `top_triples_oper.csv`: {len(triples_oper)} filas",
            f"- `top_triples_order.csv`: {len(triples_order)} filas",
        ]
    )

    path.write_text("\n".join(lines), encoding="utf-8")
    LOGGER.info("README basket generado: %s", path)
