from __future__ import annotations

import pandas as pd


def easter_sunday(year: int) -> pd.Timestamp:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return pd.Timestamp(year=year, month=month, day=day)


def add_easter_features(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    out = df.copy()
    d = pd.to_datetime(out[date_col], errors="coerce").dt.normalize()

    easter_dates = {int(y): easter_sunday(int(y)) for y in d.dt.year.dropna().unique()}

    easter_series = d.dt.year.map(easter_dates)
    rel_days = (d - easter_series).dt.days
    out["easter_week_rel"] = (rel_days / 7.0).round().astype("Int64")
    out.loc[(out["easter_week_rel"] < -4) | (out["easter_week_rel"] > 8), "easter_week_rel"] = pd.NA

    semana_santa_start = easter_series - pd.Timedelta(days=7)
    semana_santa_end = easter_series + pd.Timedelta(days=1)
    post_ss_end = easter_series + pd.Timedelta(days=14)

    out["is_semana_santa"] = ((d >= semana_santa_start) & (d <= semana_santa_end)).astype(int)
    out["post_semana_santa"] = ((d > semana_santa_end) & (d <= post_ss_end)).astype(int)
    out["easter_day_of_year"] = easter_series.dt.dayofyear
    return out
