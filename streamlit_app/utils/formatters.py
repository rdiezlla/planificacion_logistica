from __future__ import annotations

import math

import pandas as pd


def fmt_number(value: object, decimals: int = 0) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    return f"{float(value):,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_int(value: object) -> str:
    return fmt_number(value, 0)


def fmt_percent(value: object, decimals: int = 1) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    return f"{float(value) * 100:.{decimals}f}%".replace(".", ",")


def fmt_delta(value: object, decimals: int = 1) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    sign = "+" if float(value) > 0 else ""
    return f"{sign}{float(value):.{decimals}f}%".replace(".", ",")


def fmt_date(value: object) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return str(value)
    return ts.strftime("%d/%m/%Y")


def safe_text(value: object) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    text = str(value).strip()
    return text or "-"
