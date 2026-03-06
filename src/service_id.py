from __future__ import annotations

import re

import pandas as pd

CODE_SUFFIX_RE = re.compile(r"(?:-01|-02|/01)$", flags=re.IGNORECASE)
CODE_EXTRACT_RE = re.compile(r"\b(?:SGE|EGE|SGP|EGP)\d{6,}\b", flags=re.IGNORECASE)
CODE_EXACT_RE = re.compile(r"^(?:SGE|EGE|SGP|EGP)\d{6,}$", flags=re.IGNORECASE)
CODE_YEAR_RE = re.compile(r"^(?:SGE|EGE|SGP|EGP)(\d{4})\d{2,}$", flags=re.IGNORECASE)


def normalize_code(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    raw = str(value).strip().upper()
    if not raw or raw in {"NAN", "NONE", "NULL"}:
        return None
    raw = CODE_SUFFIX_RE.sub("", raw)
    return raw or None


def extract_code_from_text(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    raw = str(value).strip().upper()
    if not raw or raw in {"NAN", "NONE", "NULL"}:
        return None

    raw_no_suffix = CODE_SUFFIX_RE.sub("", raw)
    if CODE_EXACT_RE.fullmatch(raw_no_suffix):
        return raw_no_suffix

    match = CODE_EXTRACT_RE.search(raw)
    if not match:
        return None
    return normalize_code(match.group(0))


def extract_code_year(value: object) -> int | None:
    norm = normalize_code(value)
    if norm is None:
        return None
    match = CODE_YEAR_RE.match(norm)
    if not match:
        return None
    year = int(match.group(1))
    if 1900 <= year <= 2100:
        return year
    return None


def build_service_id(codigo_norm: pd.Series, fecha_servicio: pd.Series) -> pd.Series:
    fecha = pd.to_datetime(fecha_servicio, errors="coerce").dt.strftime("%Y-%m-%d")
    return codigo_norm.fillna("MISSING_CODE") + "__" + fecha.fillna("MISSING_DATE")
