from __future__ import annotations

import numpy as np
import pandas as pd


def build_holiday_calendar(dates: pd.Series, holidays_df: pd.DataFrame) -> pd.DataFrame:
    d = pd.to_datetime(dates, errors="coerce").dt.normalize()
    out = pd.DataFrame({"date": d})

    holidays = (
        pd.to_datetime(holidays_df["date"], errors="coerce")
        .dropna()
        .dt.normalize()
        .sort_values()
        .unique()
    )
    hol_set = set(holidays)

    out["is_holiday"] = out["date"].isin(hol_set).astype(int)
    out["is_pre_holiday_1"] = out["date"].add(pd.Timedelta(days=1)).isin(hol_set).astype(int)
    out["is_pre_holiday_2"] = out["date"].add(pd.Timedelta(days=2)).isin(hol_set).astype(int)
    out["is_post_holiday_1"] = out["date"].sub(pd.Timedelta(days=1)).isin(hol_set).astype(int)
    out["is_post_holiday_2"] = out["date"].sub(pd.Timedelta(days=2)).isin(hol_set).astype(int)

    dow = out["date"].dt.dayofweek
    prev_is_holiday = out["date"].sub(pd.Timedelta(days=1)).isin(hol_set)
    next_is_holiday = out["date"].add(pd.Timedelta(days=1)).isin(hol_set)
    prev_is_weekend = out["date"].sub(pd.Timedelta(days=1)).dt.dayofweek >= 5
    next_is_weekend = out["date"].add(pd.Timedelta(days=1)).dt.dayofweek >= 5

    puente = (~out["is_holiday"].astype(bool)) & (
        (prev_is_holiday & next_is_weekend)
        | (next_is_holiday & prev_is_weekend)
        | ((dow == 0) & prev_is_weekend & next_is_holiday)
        | ((dow == 4) & next_is_weekend & prev_is_holiday)
    )
    out["is_puente"] = puente.astype(int)

    if len(holidays) == 0:
        out["dias_desde_holiday"] = np.nan
        out["dias_hasta_holiday"] = np.nan
        return out

    hol_days = pd.Series(holidays).view("int64").to_numpy()
    date_days = out["date"].view("int64").to_numpy()

    pos = np.searchsorted(hol_days, date_days)

    prev_idx = np.clip(pos - 1, 0, len(hol_days) - 1)
    next_idx = np.clip(pos, 0, len(hol_days) - 1)

    prev_delta = (date_days - hol_days[prev_idx]) / (24 * 3600 * 10**9)
    next_delta = (hol_days[next_idx] - date_days) / (24 * 3600 * 10**9)

    out["dias_desde_holiday"] = np.where(prev_delta >= 0, prev_delta, np.nan)
    out["dias_hasta_holiday"] = np.where(next_delta >= 0, next_delta, np.nan)

    return out
