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
    operational_orders: Path | None


INPUT_CANDIDATES: dict[str, list[str]] = {
    "albaranes": [
        "data/raw/legacy/Informacion_albaranaes.xlsx",
        "Informacion_albaranaes.xlsx",
        "planificacion/Datos/Informacion_albaranaes.xlsx",
    ],
    "movimientos": [
        "data/raw/movimientos/movimientos.xlsx",
        "movimientos.xlsx",
        "planificacion/Datos/movimientos.xlsx",
    ],
    "holidays": ["data/holidays_madrid.csv"],
    "provincia_station_map": ["data/provincia_station_map.csv"],
}

OPTIONAL_INPUT_CANDIDATES: dict[str, list[str]] = {
    "operational_orders": [
        "data/raw/operational/lineas_solicitudes_con_pedidos.xlsx",
        "lineas_solicitudes_con_pedidos.xlsx",
    ]
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

OPERATIONAL_ALIASES = _build_alias_map(
    {
        "id": ["id"],
        "solicitud": ["solicitud"],
        "inicio_evento": ["inicio evento", "inicio_evento"],
        "creacion_solicitud": ["creación solicitud", "creacion solicitud", "creacion_solicitud"],
        "borrado_solicitud": ["borrado solicitud", "borrado_solicitud"],
        "pedido": ["pedido"],
        "articulo": ["articulo", "artículo"],
        "propietario": ["propietario"],
        "departamento": ["departamento"],
        "estado_linea": ["estado línea", "estado linea", "estado_linea"],
        "cant_solicitada": ["cant. solicitada", "cant_solicitada"],
        "cant_confirmada": ["cant. confirmada", "cant_confirmada"],
        "cant_almacenada": ["cant. almacenada", "cant_almacenada"],
        "modificacion_linea": ["modificación línea", "modificacion linea", "modificacion_linea"],
        "fin_evento": ["fin evento", "fin_evento"],
        "reservation_start_date": ["reservation_start_date"],
        "reservation_finish_date": ["reservation_finish_date"],
        "borrado_linea": ["borrado línea", "borrado linea", "borrado_linea"],
        "alta_baja": ["alta/baja", "alta_baja"],
        "comentarios": ["comentarios"],
        "propietario_solicitante": ["propietario solicitante", "propietario_solicitante"],
        "departamento_solicitante": ["departamento solicitante", "departamento_solicitante"],
        "peticionario": ["peticionario"],
        "estado": ["estado"],
        "creacion_pedido": ["creación pedido", "creacion pedido", "creacion_pedido"],
        "ultima_modificacion": ["última modificación", "ultima modificacion", "ultima_modificacion"],
        "nombre": ["nombre"],
        "apellidos": ["apellidos"],
        "telefono": ["telefono", "teléfono"],
        "localizacion": ["localización", "localizacion"],
        "ubicacion": ["ubicación", "ubicacion"],
        "provincia": ["provincia"],
        "codigo_generico": ["codigo generico", "codigo_generico"],
    }
)


def apply_header_standardization(df: pd.DataFrame, dataset: str) -> pd.DataFrame:
    if dataset == "albaranes":
        alias_map = ALBARANES_ALIASES
    elif dataset == "movimientos":
        alias_map = MOVIMIENTOS_ALIASES
    elif dataset == "operational_orders":
        alias_map = OPERATIONAL_ALIASES
    else:
        raise ValueError(f"Dataset no soportado para normalizacion de cabeceras: {dataset}")
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


def _resolve_candidate_path(root: Path, candidates: list[str]) -> Path | None:
    for rel in candidates:
        p = root / rel
        if p.exists():
            return p
    return None


def resolve_input_paths(root: Path) -> InputPaths:
    paths: dict[str, Path] = {}
    missing = []
    for key, candidates in INPUT_CANDIDATES.items():
        p = _resolve_candidate_path(root, candidates)
        if p is None:
            missing.append(f"{key}: " + " | ".join(str(root / c) for c in candidates))
            continue
        paths[key] = p
    if missing:
        raise FileNotFoundError(
            "Faltan ficheros de entrada obligatorios:\n- " + "\n- ".join(missing)
        )

    operational = _resolve_candidate_path(
        root, OPTIONAL_INPUT_CANDIDATES["operational_orders"]
    )
    if operational is None:
        LOGGER.warning(
            "No se detecto fuente operativa de pedidos (lineas_solicitudes_con_pedidos.xlsx). "
            "Se mantiene modo legacy/hibrido con fallback."
        )

    return InputPaths(
        albaranes=paths["albaranes"],
        movimientos=paths["movimientos"],
        holidays=paths["holidays"],
        provincia_station_map=paths["provincia_station_map"],
        operational_orders=operational,
    )


def load_albaranes(path: Path) -> pd.DataFrame:
    LOGGER.info("Cargando albaranes: %s", path)
    df = pd.read_excel(path)
    return apply_header_standardization(df, dataset="albaranes")


def load_movimientos(path: Path) -> pd.DataFrame:
    LOGGER.info("Cargando movimientos: %s", path)
    df = pd.read_excel(path)
    return apply_header_standardization(df, dataset="movimientos")


def load_operational_orders(path: Path) -> pd.DataFrame:
    LOGGER.info("Cargando pedidos operativos: %s", path)
    df = pd.read_excel(path)
    return apply_header_standardization(df, dataset="operational_orders")


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
