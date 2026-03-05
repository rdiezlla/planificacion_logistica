from __future__ import annotations

import logging
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
    return pd.read_excel(path)


def load_movimientos(path: Path) -> pd.DataFrame:
    LOGGER.info("Cargando movimientos: %s", path)
    return pd.read_excel(path)


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
