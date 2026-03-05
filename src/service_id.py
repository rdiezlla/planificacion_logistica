from __future__ import annotations

import re

import pandas as pd

CODE_SUFFIX_RE = re.compile(r"-\d{1,3}$")
NON_ALNUM_RE = re.compile(r"[^A-Z0-9]+")


def normalize_code(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    raw = str(value).strip().upper()
    if not raw or raw in {"NAN", "NONE", "NULL"}:
        return None
    raw = CODE_SUFFIX_RE.sub("", raw)
    raw = NON_ALNUM_RE.sub("", raw)
    return raw or None


def build_service_id(codigo_norm: pd.Series, fecha_servicio: pd.Series) -> pd.Series:
    fecha = pd.to_datetime(fecha_servicio, errors="coerce").dt.strftime("%Y-%m-%d")
    return codigo_norm.fillna("MISSING_CODE") + "__" + fecha.fillna("MISSING_DATE")
