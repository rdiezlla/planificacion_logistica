from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_STANDARDS = pd.DataFrame(
    [
        {
            "process": "picking_out",
            "driver": "picking_movs_esperados_p50",
            "driver_base": "picking_movs_esperados",
            "productivity_rate": 120.0,
            "allowance_pct": 0.15,
            "setup_hours": 0.50,
            "shift_hours": 8.0,
            "utilization_effective": 0.85,
        },
        {
            "process": "inbound_recepcion",
            "driver": "inbound_recepcion_cr_esperados_p50",
            "driver_base": "inbound_recepcion_cr_esperados",
            "productivity_rate": 22.0,
            "allowance_pct": 0.12,
            "setup_hours": 0.25,
            "shift_hours": 8.0,
            "utilization_effective": 0.85,
        },
        {
            "process": "inbound_ubicacion",
            "driver": "inbound_ubicacion_ep_esperados_p50",
            "driver_base": "inbound_ubicacion_ep_esperados",
            "productivity_rate": 28.0,
            "allowance_pct": 0.12,
            "setup_hours": 0.25,
            "shift_hours": 8.0,
            "utilization_effective": 0.85,
        },
        {
            "process": "no_atribuible",
            "driver": "picking_movs_no_atribuibles_p50",
            "driver_base": "picking_movs_no_atribuibles",
            "productivity_rate": 100.0,
            "allowance_pct": 0.15,
            "setup_hours": 0.25,
            "shift_hours": 8.0,
            "utilization_effective": 0.85,
        },
    ]
)


def _empty_staffing_frame(date_col: str) -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            date_col,
            "process",
            "driver",
            "driver_forecast_p50",
            "driver_forecast_p80",
            "productivity_estandar",
            "allowance_pct",
            "setup_hours",
            "shift_hours",
            "utilization_effective",
            "horas_requeridas_p50",
            "horas_requeridas_p80",
            "fte_requeridos_p50",
            "fte_requeridos_p80",
        ]
    )


def load_labor_standards(path: Path) -> pd.DataFrame:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        DEFAULT_STANDARDS.to_csv(path, index=False)
        return DEFAULT_STANDARDS.copy()

    df = pd.read_csv(path)
    required = [
        "process",
        "driver",
        "productivity_rate",
        "allowance_pct",
        "setup_hours",
        "shift_hours",
        "utilization_effective",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"labor_standards.csv sin columnas requeridas: {missing}")
    if "driver_base" not in df.columns:
        df["driver_base"] = df["driver"].astype(str).str.replace("_p50", "", regex=False)
        df["driver_base"] = df["driver_base"].str.replace("_p80", "", regex=False)
    return df


def build_staffing_plan(
    plan_df: pd.DataFrame,
    standards_df: pd.DataFrame,
    *,
    date_col: str,
) -> pd.DataFrame:
    if plan_df.empty or standards_df.empty:
        return _empty_staffing_frame(date_col)

    df = plan_df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
    rows: list[dict[str, object]] = []
    def _resolve_driver_columns(frame: pd.DataFrame, driver_base: str, configured_driver: str) -> tuple[str | None, str | None]:
        candidates_p50 = [
            configured_driver if configured_driver.endswith("_p50") else None,
            f"{driver_base}_p50" if driver_base else None,
        ]
        candidates_p80 = [
            configured_driver.replace("_p50", "_p80") if configured_driver.endswith("_p50") else None,
            f"{driver_base}_p80" if driver_base else None,
        ]

        for suffix in ["_semana_p50", "_daily_p50", "_weekly_p50"]:
            if driver_base:
                candidates_p50.append(f"{driver_base}{suffix}")
        for suffix in ["_semana_p80", "_daily_p80", "_weekly_p80"]:
            if driver_base:
                candidates_p80.append(f"{driver_base}{suffix}")

        dynamic_p50 = sorted([c for c in frame.columns if driver_base and c.startswith(driver_base) and c.endswith("_p50")])
        dynamic_p80 = sorted([c for c in frame.columns if driver_base and c.startswith(driver_base) and c.endswith("_p80")])
        candidates_p50.extend(dynamic_p50)
        candidates_p80.extend(dynamic_p80)

        p50_col = next((c for c in candidates_p50 if c and c in frame.columns), None)
        p80_col = next((c for c in candidates_p80 if c and c in frame.columns), None)
        return p50_col, p80_col

    for _, std in standards_df.iterrows():
        driver_base = str(std.get("driver_base") or "").strip()
        if not driver_base:
            continue
        configured_driver = str(std.get("driver") or "").strip()
        driver_p50, driver_p80 = _resolve_driver_columns(df, driver_base, configured_driver)
        if driver_p50 is None and driver_p80 is None:
            continue

        prod = float(pd.to_numeric(std["productivity_rate"], errors="coerce"))
        allowance = float(pd.to_numeric(std["allowance_pct"], errors="coerce"))
        setup = float(pd.to_numeric(std["setup_hours"], errors="coerce"))
        shift_hours = float(pd.to_numeric(std["shift_hours"], errors="coerce"))
        util = float(pd.to_numeric(std["utilization_effective"], errors="coerce"))
        denom_fte = max(shift_hours * util, 1e-6)

        for _, rec in df.iterrows():
            driver_forecast_p50 = float(pd.to_numeric(rec.get(driver_p50), errors="coerce")) if driver_p50 else np.nan
            driver_forecast_p80 = float(pd.to_numeric(rec.get(driver_p80), errors="coerce")) if driver_p80 else np.nan
            driver_forecast_p50 = max(driver_forecast_p50, 0.0) if np.isfinite(driver_forecast_p50) else 0.0
            driver_forecast_p80 = max(driver_forecast_p80, 0.0) if np.isfinite(driver_forecast_p80) else driver_forecast_p50
            horas_p50 = ((driver_forecast_p50 / max(prod, 1e-6)) * (1.0 + allowance)) + setup
            horas_p80 = ((driver_forecast_p80 / max(prod, 1e-6)) * (1.0 + allowance)) + setup
            rows.append(
                {
                    date_col: rec[date_col],
                    "process": std["process"],
                    "driver": driver_base,
                    "driver_forecast_p50": driver_forecast_p50,
                    "driver_forecast_p80": max(driver_forecast_p80, driver_forecast_p50),
                    "productivity_estandar": prod,
                    "allowance_pct": allowance,
                    "setup_hours": setup,
                    "shift_hours": shift_hours,
                    "utilization_effective": util,
                    "horas_requeridas_p50": horas_p50,
                    "horas_requeridas_p80": max(horas_p80, horas_p50),
                    "fte_requeridos_p50": horas_p50 / denom_fte,
                    "fte_requeridos_p80": max(horas_p80, horas_p50) / denom_fte,
                }
            )
    out = pd.DataFrame(rows)
    if out.empty:
        return _empty_staffing_frame(date_col)
    return out.sort_values([date_col, "process"]).reset_index(drop=True)
