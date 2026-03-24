from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from src.io import resolve_input_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orquesta forecast + ABC + basket usando las fuentes de OneDrive."
    )
    parser.add_argument("--refresh_clean_inputs", action="store_true")
    parser.add_argument("--skip_forecast", action="store_true")
    parser.add_argument("--skip_abc", action="store_true")
    parser.add_argument("--skip_basket", action="store_true")
    parser.add_argument("--dry_run", action="store_true")
    parser.add_argument("--horizon_days", type=int, default=60)
    parser.add_argument("--freq", type=str, default="both", choices=["daily", "weekly", "both"])
    parser.add_argument(
        "--data_mode",
        type=str,
        default="hybrid",
        choices=["legacy", "hybrid", "operational-first"],
    )
    parser.add_argument("--operational_cutover_date", type=str, default=None)
    parser.add_argument("--basket_min_support", type=float, default=0.005)
    parser.add_argument("--basket_min_conf", type=float, default=0.2)
    parser.add_argument("--basket_min_lift", type=float, default=1.2)
    parser.add_argument("--basket_max_basket_size", type=int, default=80)
    return parser.parse_args()


def run_command(cmd: list[str], cwd: Path, dry_run: bool = False) -> None:
    printable = " ".join(f'"{part}"' if " " in part else part for part in cmd)
    print(f"[RUN] {printable}")
    if dry_run:
        return
    subprocess.run(cmd, cwd=str(cwd), check=True)


def print_sources(root: Path) -> None:
    paths = resolve_input_paths(root)
    rows = [
        ("albaranes", paths.albaranes),
        ("movimientos_consolidado", paths.movimientos),
        ("pedidos_operativos", paths.operational_orders),
        ("maestro_dimensiones", paths.master_dimensions),
        ("raw_movimientos_dir", paths.raw_movimientos_dir),
        ("raw_movimientos_pedidos_dir", paths.raw_movimientos_pedidos_dir),
        ("cleanup_movimientos_notebook", paths.cleanup_movimientos_notebook),
        ("cleanup_pedidos_notebook", paths.cleanup_pedidos_notebook),
        ("cleanup_general_script", paths.cleanup_general_script),
        ("download_movimientos_script", paths.download_movimientos_script),
    ]
    print("[INFO] Fuentes resueltas")
    for label, value in rows:
        print(f"  - {label}: {value if value is not None else 'NO DISPONIBLE'}")


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parent
    python_exe = sys.executable

    print_sources(root)

    paths = resolve_input_paths(root)
    if args.refresh_clean_inputs and paths.cleanup_general_script is not None:
        run_command([python_exe, str(paths.cleanup_general_script)], cwd=root, dry_run=args.dry_run)
    elif args.refresh_clean_inputs:
        print("[WARN] No se encontro limpieza_general.py; se omite refresco de inputs limpios.")

    if not args.skip_forecast:
        forecast_cmd = [
            python_exe,
            "main.py",
            "--horizon_days",
            str(args.horizon_days),
            "--freq",
            args.freq,
            "--data_mode",
            args.data_mode,
        ]
        if args.operational_cutover_date:
            forecast_cmd.extend(["--operational_cutover_date", args.operational_cutover_date])
        run_command(forecast_cmd, cwd=root, dry_run=args.dry_run)

    if not args.skip_abc:
        run_command([python_exe, "abc_main.py", "--output_dir", "outputs_abc"], cwd=root, dry_run=args.dry_run)

    if not args.skip_basket:
        run_command(
            [
                python_exe,
                "basket_main.py",
                "--output_dir",
                "outputs_basket",
                "--min_support",
                str(args.basket_min_support),
                "--min_conf",
                str(args.basket_min_conf),
                "--min_lift",
                str(args.basket_min_lift),
                "--max_basket_size",
                str(args.basket_max_basket_size),
            ],
            cwd=root,
            dry_run=args.dry_run,
        )

    print("[OK] Flujo diario completado.")


if __name__ == "__main__":
    main()
