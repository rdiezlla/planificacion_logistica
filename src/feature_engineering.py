from __future__ import annotations

import logging
from typing import Iterable

import numpy as np
import pandas as pd

from .calendar_features import add_calendar_features
from .easter import add_easter_features
from .holidays import build_holiday_calendar
from .weather_aemet import extend_weather_with_climatology

LOGGER = logging.getLogger(__name__)


DEFAULT_EXCLUDE_YEARS = [2025]


def parse_exclude_years(value: str | Iterable[int] | None) -> list[int]:
    if value is None:
        return DEFAULT_EXCLUDE_YEARS
    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",") if p.strip()]
        years = []
        for p in parts:
            try:
                years.append(int(p))
            except ValueError:
                LOGGER.warning("exclude_years contiene valor no numerico: %s", p)
        return years
    return [int(v) for v in value]


def add_time_features(
    target_df: pd.DataFrame,
    holidays_df: pd.DataFrame,
    weather_daily: pd.DataFrame | None = None,
) -> pd.DataFrame:
    out = target_df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out = out.dropna(subset=["date"]).copy()

    out = add_calendar_features(out, date_col="date")

    h = build_holiday_calendar(out["date"], holidays_df).drop_duplicates(subset=["date"])
    out = out.merge(h, on="date", how="left")

    out = add_easter_features(out, date_col="date")

    if weather_daily is not None:
        weather_for_dates = extend_weather_with_climatology(weather_daily, out["date"])
        out = out.merge(weather_for_dates, on="date", how="left")
    else:
        for c in [
            "temp_media_weighted",
            "precip_weighted",
            "viento_weighted",
            "temp_media_weighted_roll3",
            "temp_media_weighted_roll7",
            "precip_weighted_roll3",
            "precip_weighted_roll7",
            "viento_weighted_roll3",
            "viento_weighted_roll7",
        ]:
            out[c] = np.nan
        out["weather_source"] = "missing"
        out["weather_fallback"] = 1

    return out


def split_history_future(
    df: pd.DataFrame,
    cutoff_date: pd.Timestamp,
    exclude_years: list[int] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out = out.dropna(subset=["date"]).copy()

    years = exclude_years or []
    out["excluded_year"] = out["date"].dt.year.isin(years).astype(int)

    hist = out[(out["date"] <= cutoff_date) & out["excluded_year"].eq(0)].copy()
    future = out[out["date"] > cutoff_date].copy()
    return hist, future


def generate_future_frame(
    start_date: pd.Timestamp,
    horizon_days: int,
    axis: str,
    tipo_servicios: list[str] | None = None,
) -> pd.DataFrame:
    dates = pd.date_range(start_date, periods=horizon_days, freq="D")
    if axis == "service":
        tipos = tipo_servicios or ["entrega", "recogida", "mixto", "desconocida"]
        rows = []
        for d in dates:
            for t in tipos:
                rows.append({"date": d, "axis": axis, "tipo_servicio": t})
        return pd.DataFrame(rows)

    return pd.DataFrame({"date": dates, "axis": axis, "tipo_servicio": "ALL"})


def aggregate_weekly(df: pd.DataFrame, sum_cols: list[str], keep_cols: list[str] | None = None) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out["week_start"] = out["date"] - pd.to_timedelta(out["date"].dt.dayofweek, unit="D")

    group_cols = ["week_start"]
    if "tipo_servicio" in out.columns:
        group_cols.append("tipo_servicio")
    if keep_cols:
        for c in keep_cols:
            if c in out.columns and c not in group_cols:
                group_cols.append(c)

    agg = {c: "sum" for c in sum_cols if c in out.columns}
    weekly = out.groupby(group_cols, dropna=False).agg(agg).reset_index().rename(columns={"week_start": "date"})
    return weekly
