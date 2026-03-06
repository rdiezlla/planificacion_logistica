from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class InputPaths:
    albaranes: Path
    movimientos: Path
    holidays: Path
    provincia_station_map: Path


REQUIRED_INPUTS = {
    "albaranes": "Informacion_albaranaes.xlsx",
    "movimientos": "movimientos.xlsx",
    "holidays": "data/holidays_madrid.csv",
    "provincia_station_map": "data/provincia_station_map.csv",
}


def normalize_header_name(name: object) -> str:
    text = "" if name is None else str(name)
    text = text.strip().lower()
    text = "".join(
        c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn"
    )
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_]", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _build_alias_map(canonical_aliases: dict[str, list[str]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for canonical, aliases in canonical_aliases.items():
        for alias in aliases:
            out[normalize_header_name(alias)] = canonical
        out[normalize_header_name(canonical)] = canonical
    return out


ALBARANES_ALIASES = _build_alias_map(
    {
        "item": ["item"],
        "fecha_servicio": ["fecha servicio", "fecha_servicio", "fecha_servicio_"],
        "descripcion": ["descripcion", "descripción"],
        "concepto_evento": [
            "concepto (denominación evento asociado)",
            "concepto (denominacion evento asociado)",
            "concepto_denominacion_evento_asociado",
            "concepto_evento",
            "concepto",
        ],
        "urgencia": ["urgencia"],
        "festivo": ["festivo"],
        "provincia_destino": ["provincia destino", "provincia_destino"],
        "m3_in": ["m3 in", "m3_in"],
        "m3_out": ["m3 out", "m3_out"],
        "cajas_in": ["cajas in", "cajas_in"],
        "cajas_out": ["cajas out", "cajas_out"],
        "pales_in": ["palés in", "pales in", "pales_in"],
        "pales_out": ["palés out", "pales out", "pales_out"],
        "peso_kg": ["peso (kg)", "peso_kg", "peso kg"],
        "volumen": ["volumen", "volumen_m3_total", "volumen m3 total"],
    }
)

MOVIMIENTOS_ALIASES = _build_alias_map(
    {
        "tipo_movimiento": ["tipo movimiento", "tipo_movimiento"],
        "fecha_inicio": ["fecha inicio", "fecha_inicio"],
        "fecha_finalizacion": ["fecha finalización", "fecha finalizacion", "fecha_finalizacion"],
        "articulo": ["artículo", "articulo"],
        "cantidad": ["cantidad"],
        "pedido": ["pedido"],
        "pedido_externo": ["pedido externo", "pedido_externo"],
        "cliente": ["cliente"],
        "cliente_externo": ["cliente externo", "cliente_externo"],
        "denominacion_articulo": ["denominación artículo", "denominacion articulo", "denominacion_articulo"],
        "denominacion_operario": ["denominación operario", "denominacion operario", "denominacion_operario"],
    }
)


def apply_header_standardization(df: pd.DataFrame, dataset: str) -> pd.DataFrame:
    alias_map = ALBARANES_ALIASES if dataset == "albaranes" else MOVIMIENTOS_ALIASES
    normalized_headers = [normalize_header_name(c) for c in df.columns]
    canonical_headers = [alias_map.get(h, h) for h in normalized_headers]

    # Consolidates duplicate columns created by aliases, keeping first non-null value.
    idx_map: dict[str, list[int]] = {}
    for idx, col in enumerate(canonical_headers):
        idx_map.setdefault(col, []).append(idx)

    out = pd.DataFrame(index=df.index)
    for col, idxs in idx_map.items():
        series = df.iloc[:, idxs[0]].copy()
        for idx in idxs[1:]:
            candidate = df.iloc[:, idx]
            series = series.where(series.notna(), candidate)
        out[col] = series
    return out


def resolve_input_paths(root: Path) -> InputPaths:
    paths = {}
    missing = []
    for key, rel in REQUIRED_INPUTS.items():
        p = root / rel
        paths[key] = p
        if not p.exists():
            missing.append(str(p))
    if missing:
        raise FileNotFoundError(
            "Faltan ficheros de entrada obligatorios:\n- " + "\n- ".join(missing)
        )
    return InputPaths(
        albaranes=paths["albaranes"],
        movimientos=paths["movimientos"],
        holidays=paths["holidays"],
        provincia_station_map=paths["provincia_station_map"],
    )


def load_albaranes(path: Path) -> pd.DataFrame:
    LOGGER.info("Cargando albaranes: %s", path)
    df = pd.read_excel(path)
    return apply_header_standardization(df, dataset="albaranes")


def load_movimientos(path: Path) -> pd.DataFrame:
    LOGGER.info("Cargando movimientos: %s", path)
    df = pd.read_excel(path)
    return apply_header_standardization(df, dataset="movimientos")


def load_holidays(path: Path) -> pd.DataFrame:
    LOGGER.info("Cargando festivos: %s", path)
    df = pd.read_csv(path)
    if "date" not in df.columns:
        raise ValueError(f"holidays_madrid.csv sin columna 'date': {path}")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["date"]).copy()
    return df


def load_provincia_station_map(path: Path) -> pd.DataFrame:
    LOGGER.info("Cargando mapeo provincia->capital: %s", path)
    df = pd.read_csv(path)
    required = {"provincia_norm", "capital"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"provincia_station_map.csv sin columnas requeridas {sorted(missing)}: {path}"
        )
    return df
