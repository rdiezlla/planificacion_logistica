from __future__ import annotations

import pandas as pd


def add_calendar_features(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    out = df.copy()
    d = pd.to_datetime(out[date_col], errors="coerce")

    out["dow"] = d.dt.dayofweek
    out["week_iso"] = d.dt.isocalendar().week.astype(int)
    out["month"] = d.dt.month
    out["year"] = d.dt.year
    out["day"] = d.dt.day
    out["week_of_month"] = ((d.dt.day - 1) // 7 + 1).astype(int)
    out["fin_de_mes"] = d.dt.is_month_end.astype(int)
    out["is_weekend"] = d.dt.dayofweek.isin([5, 6]).astype(int)
    return out
