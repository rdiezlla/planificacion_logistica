from __future__ import annotations

import unicodedata

import pandas as pd

EXPLICIT_PROVINCE_MAP = {
    "CORUNA": "A_CORUNA",
    "LA_CORUNA": "A_CORUNA",
    "ALAVA": "ALAVA",
    "LERIDA": "LLEIDA",
    "GERONA": "GIRONA",
    "GUIPUZCOA": "GIPUZKOA",
    "VIZCAYA": "BIZKAIA",
    "CADIZ": "CADIZ",
    "AVILA": "AVILA",
    "LEON": "LEON",
    "JAEN": "JAEN",
    "MALAGA": "MALAGA",
}


def _ascii_upper(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = str(value).strip().upper()
    s = "".join(
        ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn"
    )
    s = s.replace("-", " ").replace("/", " ")
    s = "_".join(s.split())
    return s


def build_province_normalizer(provincia_station_map: pd.DataFrame) -> dict[str, str]:
    mapping = {}
    for raw in provincia_station_map["provincia_norm"].dropna().astype(str):
        key = _ascii_upper(raw)
        mapping[key] = raw
    mapping.update(EXPLICIT_PROVINCE_MAP)
    return mapping


def normalize_provincia_destino(
    series: pd.Series,
    provincia_station_map: pd.DataFrame,
) -> pd.Series:
    mapping = build_province_normalizer(provincia_station_map)

    def _norm(v: object) -> str:
        key = _ascii_upper(v)
        if not key or key in {"NAN", "NONE", "NULL"}:
            return "DESCONOCIDA"
        return mapping.get(key, key)

    return series.map(_norm)
