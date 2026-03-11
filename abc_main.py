from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from src_abc.abc_compare import build_layout_candidates, build_top_changes
from src_abc.abc_core import (
    AbcThresholds,
    XyzThresholds,
    build_abc_xyz_summary_by_period,
    build_annual_abc,
    build_owner_summary,
    build_quarterly_abc,
    build_summary_by_period,
    build_ytd_abc,
    determine_owner_scopes,
)
from src_abc.load import load_abc_picking_lines
from src_abc.report import generate_abc_plots, write_readme_abc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analisis Pareto / ABC de picking")
    parser.add_argument("--input", default="movimientos.xlsx", help="Ruta al Excel de movimientos")
    parser.add_argument("--output_dir", default="outputs_abc", help="Carpeta de salida")
    parser.add_argument("--a-threshold", type=float, default=0.80, dest="a_threshold", help="Umbral acumulado para clase A")
    parser.add_argument("--b-threshold", type=float, default=0.95, dest="b_threshold", help="Umbral acumulado para clase B")
    parser.add_argument("--xyz-x-threshold", type=float, default=0.50, dest="xyz_x_threshold", help="Umbral de estabilidad X sobre cv semanal")
    parser.add_argument("--xyz-y-threshold", type=float, default=1.00, dest="xyz_y_threshold", help="Umbral de estabilidad Y sobre cv semanal")
    parser.add_argument("--max_owners", type=int, default=10, help="Numero maximo de propietarios especificos a incluir, ademas de GLOBAL")
    parser.add_argument("--log_level", default="INFO", help="Nivel de log")
    return parser.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)
    logger = logging.getLogger("abc_main")

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "plots").mkdir(parents=True, exist_ok=True)

    logger.info("Iniciando analisis ABC picking | input=%s | output=%s", input_path, output_dir)
    abc_thresholds = AbcThresholds(a_threshold=args.a_threshold, b_threshold=args.b_threshold)
    xyz_thresholds = XyzThresholds(x_threshold=args.xyz_x_threshold, y_threshold=args.xyz_y_threshold)
    lines, stats = load_abc_picking_lines(input_path)
    owner_scopes = determine_owner_scopes(lines, max_owners=args.max_owners)
    owner_summary_df = build_owner_summary(lines, owner_scopes)

    annual_df = build_annual_abc(lines, abc_thresholds, xyz_thresholds, owner_scopes)
    quarterly_df = build_quarterly_abc(lines, abc_thresholds, xyz_thresholds, owner_scopes)
    ytd_df = build_ytd_abc(lines, abc_thresholds, xyz_thresholds, owner_scopes)

    period_df = pd.concat([annual_df, quarterly_df, ytd_df], ignore_index=True)
    summary_df = build_summary_by_period(period_df)
    abc_xyz_summary_df = build_abc_xyz_summary_by_period(period_df)
    top_changes_df = build_top_changes(period_df)
    layout_df = build_layout_candidates(period_df, top_changes_df)

    save_csv(annual_df, output_dir / "abc_picking_annual.csv")
    save_csv(quarterly_df, output_dir / "abc_picking_quarterly.csv")
    save_csv(ytd_df, output_dir / "abc_picking_ytd.csv")
    save_csv(summary_df, output_dir / "abc_summary_by_period.csv")
    save_csv(abc_xyz_summary_df, output_dir / "abc_xyz_summary_by_period.csv")
    save_csv(owner_summary_df, output_dir / "abc_owner_summary.csv")
    save_csv(top_changes_df, output_dir / "abc_top_changes.csv")
    save_csv(layout_df, output_dir / "abc_for_layout_candidates.csv")

    generate_abc_plots(
        output_dir,
        annual_df=annual_df,
        quarterly_df=quarterly_df,
        summary_df=summary_df,
        top_changes_df=top_changes_df,
        layout_df=layout_df,
    )
    write_readme_abc(Path("README_ABC.md"), stats, summary_df, abc_xyz_summary_df, owner_summary_df, layout_df, top_changes_df)
    logger.info("Analisis ABC completado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
