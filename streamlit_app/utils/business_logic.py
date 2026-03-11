from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd


OUT_WEEKLY = ["eventos_entrega_semana", "m3_out_semana", "pales_out_semana", "cajas_out_semana", "peso_facturable_out_semana"]
IN_WEEKLY = ["eventos_recogida_semana", "m3_in_semana", "pales_in_semana", "cajas_in_semana", "peso_facturable_in_semana"]
PICKING_WEEKLY = ["picking_movs_esperados_semana"]

OUT_DAILY = ["eventos_entrega", "m3_out", "pales_out", "cajas_out", "peso_facturable_out"]
IN_DAILY = ["eventos_recogida", "m3_in", "pales_in", "cajas_in", "peso_facturable_in"]
PICKING_DAILY = ["picking_movs_esperados"]


@dataclass(frozen=True)
class RangeSelection:
    preset: str
    start_date: date | None = None
    end_date: date | None = None


def add_date_columns(df: pd.DataFrame, frequency: str) -> pd.DataFrame:
    out = df.copy()
    date_col = "fecha" if frequency == "daily" else "week_start_date"
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    if frequency == "weekly" and "week_end_date" in out.columns:
        out["week_end_date"] = pd.to_datetime(out["week_end_date"], errors="coerce")
    return out.dropna(subset=[date_col]).sort_values(date_col).reset_index(drop=True)


def filter_range(df: pd.DataFrame, frequency: str, selection: RangeSelection) -> pd.DataFrame:
    if df.empty:
        return df
    date_col = "fecha" if frequency == "daily" else "week_start_date"
    out = add_date_columns(df, frequency)
    if selection.preset == "custom" and selection.start_date and selection.end_date:
        start = pd.Timestamp(selection.start_date)
        end = pd.Timestamp(selection.end_date)
        return out.loc[out[date_col].between(start, end)].copy()

    periods_map = {"8 semanas": 8, "12 semanas": 12, "26 semanas": 26}
    if frequency == "weekly" and selection.preset in periods_map:
        return out.tail(periods_map[selection.preset]).copy()
    if frequency == "daily" and selection.preset in periods_map:
        return out.tail(periods_map[selection.preset] * 7).copy()
    return out.copy()


def current_vs_previous(series: pd.Series) -> tuple[float | None, float | None]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if len(values) < 2:
        return None, None
    current = float(values.iloc[-1])
    previous = float(values.iloc[-2])
    if previous == 0:
        return current, None
    return current, (current - previous) / previous


def current_vs_mean(series: pd.Series, lookback: int = 8) -> tuple[float | None, float | None]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if len(values) < 2:
        return None, None
    current = float(values.iloc[-1])
    ref = float(values.iloc[-(lookback + 1):-1].mean()) if len(values) > 1 else 0.0
    if ref == 0:
        return current, None
    return current, (current - ref) / ref


def period_mask(df: pd.DataFrame, frequency: str) -> pd.Series:
    date_col = "fecha" if frequency == "daily" else "week_start_date"
    dates = pd.to_datetime(df[date_col], errors="coerce")
    return dates.dt.date <= pd.Timestamp.today().date()


def top_weeks(df: pd.DataFrame, metric: str, n: int = 5) -> pd.DataFrame:
    if df.empty or metric not in df.columns:
        return pd.DataFrame()
    return df.sort_values(metric, ascending=False).head(n).copy()


def infer_last_run_timestamp(paths_info: list[dict[str, object]]) -> pd.Timestamp | None:
    valid = [info["modified"] for info in paths_info if info.get("exists") and pd.notna(info.get("modified"))]
    if not valid:
        return None
    return max(valid)
