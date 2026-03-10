from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src_basket.association_rules import mine_rules
from src_basket.build_transactions import build_transaction_views
from src_basket.clustering import build_sku_clusters
from src_basket.cooccurrence import compute_top_pairs
from src_basket.load import load_picking_lines
from src_basket.owner_penalty import build_owner_penalty
from src_basket.report import estimate_location_savings, generate_plots, write_readme_basket


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market basket analysis para picking")
    parser.add_argument("--input", default="movimientos.xlsx", help="Ruta al Excel de movimientos")
    parser.add_argument("--output_dir", default="outputs_basket", help="Carpeta de salida")
    parser.add_argument("--min_support", type=float, default=0.005, help="Soporte minimo")
    parser.add_argument("--min_conf", type=float, default=0.2, help="Confianza minima")
    parser.add_argument("--min_lift", type=float, default=1.2, help="Lift minimo")
    parser.add_argument("--max_basket_size", type=int, default=80, help="Tamano maximo de cesta")
    parser.add_argument("--log_level", default="INFO", help="Nivel de log")
    return parser.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def save_csv(df, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)
    logger = logging.getLogger("basket_main")

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "plots").mkdir(parents=True, exist_ok=True)

    logger.info("Iniciando basket analysis | input=%s | output=%s", input_path, output_dir)
    lines, load_stats = load_picking_lines(input_path)
    views = build_transaction_views(lines, max_basket_size=args.max_basket_size)

    for level, view in views.items():
        save_csv(view.summary_all, output_dir / f"transactions_summary_{level}.csv")
        save_csv(view.sku_frequency, output_dir / f"sku_frequency_{level}.csv")

    pairs_oper = compute_top_pairs(views["oper"].items_eligible, views["oper"].sku_frequency)
    pairs_order = compute_top_pairs(views["order"].items_eligible, views["order"].sku_frequency)
    save_csv(pairs_oper, output_dir / "top_pairs_oper.csv")
    save_csv(pairs_order, output_dir / "top_pairs_order.csv")

    rules_oper, triples_oper = mine_rules(
        views["oper"].items_eligible,
        min_support=args.min_support,
        min_confidence=args.min_conf,
        min_lift=args.min_lift,
    )
    rules_order, triples_order = mine_rules(
        views["order"].items_eligible,
        min_support=args.min_support,
        min_confidence=args.min_conf,
        min_lift=args.min_lift,
    )
    save_csv(rules_oper, output_dir / "rules_oper.csv")
    save_csv(rules_order, output_dir / "rules_order.csv")
    save_csv(triples_oper, output_dir / "top_triples_oper.csv")
    save_csv(triples_order, output_dir / "top_triples_order.csv")

    clusters_oper = build_sku_clusters(pairs_oper, views["oper"].sku_frequency)
    clusters_order = build_sku_clusters(pairs_order, views["order"].sku_frequency)
    save_csv(clusters_oper, output_dir / "sku_clusters_oper.csv")
    save_csv(clusters_order, output_dir / "sku_clusters_order.csv")

    owner_penalty, owner_kpis = build_owner_penalty(lines)
    save_csv(owner_penalty, output_dir / "order_owner_penalty.csv")
    save_csv(owner_kpis, output_dir / "owner_penalty_kpis.csv")

    location_savings = estimate_location_savings(lines, pairs_oper)
    if not location_savings.empty:
        save_csv(location_savings, output_dir / "location_savings_oper.csv")

    generate_plots(
        output_dir,
        views["oper"].summary_eligible,
        views["order"].summary_eligible,
        pairs_oper,
        pairs_order,
        views["oper"].sku_frequency,
        views["order"].sku_frequency,
    )
    write_readme_basket(
        Path("README_BASKET.md"),
        load_stats=load_stats,
        views=views,
        pairs_oper=pairs_oper,
        pairs_order=pairs_order,
        triples_oper=triples_oper,
        triples_order=triples_order,
        clusters_oper=clusters_oper,
        penalty_kpis=owner_kpis,
        location_savings=location_savings,
    )
    logger.info("Basket analysis completado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
