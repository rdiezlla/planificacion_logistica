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
    master_dimensions: Path | None
    raw_movimientos_dir: Path | None
    raw_movimientos_pedidos_dir: Path | None
    cleanup_movimientos_notebook: Path | None
    cleanup_pedidos_notebook: Path | None
    cleanup_general_script: Path | None
    download_movimientos_script: Path | None


ONEDRIVE_BASE_DIR = (
    Path.home()
    / "OneDrive - Severiano Servicio Móvil S.A.U"
    / "RIVAS - ALMACÉN, TRANSPORTE Y EVENTOS - General"
    / "pruebas"
)
ONEDRIVE_DESCARGAS_BI_DIR = ONEDRIVE_BASE_DIR / "Descargas BI"
ONEDRIVE_PLANIFICACION_DIR = ONEDRIVE_BASE_DIR / "Descargas BI" / "planificacion"


INPUT_CANDIDATES: dict[str, list[str]] = {
    "albaranes": [
        str(ONEDRIVE_DESCARGAS_BI_DIR / "Informacion_albaranaes.xlsx"),
        str(ONEDRIVE_PLANIFICACION_DIR / "Datos" / "Informacion_albaranaes.xlsx"),
        "planificacion/Datos/Informacion_albaranaes.xlsx",
        "data/raw/legacy/Informacion_albaranaes.xlsx",
        "Informacion_albaranaes.xlsx",
    ],
    "movimientos": [
        str(ONEDRIVE_DESCARGAS_BI_DIR / "movimientos.xlsx"),
        str(ONEDRIVE_PLANIFICACION_DIR / "Datos" / "movimientos.xlsx"),
        "planificacion/Datos/movimientos.xlsx",
        "data/raw/movimientos/movimientos.xlsx",
        "movimientos.xlsx",
    ],
    "holidays": ["data/holidays_madrid.csv"],
    "provincia_station_map": ["data/provincia_station_map.csv"],
}

OPTIONAL_INPUT_CANDIDATES: dict[str, list[str]] = {
    "operational_orders": [
        str(ONEDRIVE_DESCARGAS_BI_DIR / "lineas_solicitudes_con_pedidos.xlsx"),
        str(ONEDRIVE_BASE_DIR / "lineas_solicitudes_con_pedidos.xlsx"),
        "data/raw/operational/lineas_solicitudes_con_pedidos.xlsx",
        "lineas_solicitudes_con_pedidos.xlsx",
    ],
    "master_dimensions": [
        str(ONEDRIVE_DESCARGAS_BI_DIR / "maestro_dimensiones_limpio.xlsx"),
        str(ONEDRIVE_BASE_DIR / "maestro_dimensiones_limpio.xlsx"),
        "maestro_dimensiones_limpio.xlsx",
    ],
    "raw_movimientos_dir": [
        str(ONEDRIVE_DESCARGAS_BI_DIR / "Movimientos"),
        str(ONEDRIVE_PLANIFICACION_DIR / "Datos" / "Movimientos"),
        "planificacion/Datos/Movimientos",
    ],
    "raw_movimientos_pedidos_dir": [
        str(ONEDRIVE_DESCARGAS_BI_DIR / "Movimientos pedidos"),
        str(ONEDRIVE_PLANIFICACION_DIR / "Datos" / "Movimientos pedidos"),
        "planificacion/Datos/Movimientos pedidos",
    ],
    "cleanup_movimientos_notebook": [
        str(ONEDRIVE_DESCARGAS_BI_DIR / "limpieza_movimientos.ipynb"),
        "planificacion/limpieza_movimientos.ipynb",
    ],
    "cleanup_pedidos_notebook": [
        str(ONEDRIVE_DESCARGAS_BI_DIR / "limpieza_pedidos.ipynb"),
        "planificacion/limpieza_pedidos.ipynb",
    ],
    "cleanup_general_script": [
        str(ONEDRIVE_BASE_DIR / "limpieza_general.py"),
    ],
    "download_movimientos_script": [
        str(ONEDRIVE_BASE_DIR / "movimientos.py"),
    ],
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
    for raw_candidate in candidates:
        candidate = Path(raw_candidate).expanduser()
        p = candidate if candidate.is_absolute() else root / candidate
        if p.exists():
            return p.resolve()
    return None


def resolve_movimientos_input_path(root: Path, input_override: str | None = None) -> Path:
    if input_override:
        override_path = Path(input_override).expanduser()
        if not override_path.is_absolute():
            override_path = (root / override_path).resolve()
        if not override_path.exists():
            raise FileNotFoundError(f"No existe el fichero de movimientos indicado: {override_path}")
        return override_path
    return resolve_input_paths(root).movimientos


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
    master_dimensions = _resolve_candidate_path(
        root, OPTIONAL_INPUT_CANDIDATES["master_dimensions"]
    )
    raw_movimientos_dir = _resolve_candidate_path(
        root, OPTIONAL_INPUT_CANDIDATES["raw_movimientos_dir"]
    )
    raw_movimientos_pedidos_dir = _resolve_candidate_path(
        root, OPTIONAL_INPUT_CANDIDATES["raw_movimientos_pedidos_dir"]
    )
    cleanup_movimientos_notebook = _resolve_candidate_path(
        root, OPTIONAL_INPUT_CANDIDATES["cleanup_movimientos_notebook"]
    )
    cleanup_pedidos_notebook = _resolve_candidate_path(
        root, OPTIONAL_INPUT_CANDIDATES["cleanup_pedidos_notebook"]
    )
    cleanup_general_script = _resolve_candidate_path(
        root, OPTIONAL_INPUT_CANDIDATES["cleanup_general_script"]
    )
    download_movimientos_script = _resolve_candidate_path(
        root, OPTIONAL_INPUT_CANDIDATES["download_movimientos_script"]
    )
    if operational is None:
        LOGGER.warning(
            "No se detecto fuente operativa de pedidos (lineas_solicitudes_con_pedidos.xlsx). "
            "Se mantiene modo legacy/hibrido con fallback."
        )
    if master_dimensions is None:
        LOGGER.info(
            "No se detecto maestro_dimensiones_limpio.xlsx. "
            "La resolucion de rutas queda preparada, pero el pipeline actual no lo consume todavia."
        )

    return InputPaths(
        albaranes=paths["albaranes"],
        movimientos=paths["movimientos"],
        holidays=paths["holidays"],
        provincia_station_map=paths["provincia_station_map"],
        operational_orders=operational,
        master_dimensions=master_dimensions,
        raw_movimientos_dir=raw_movimientos_dir,
        raw_movimientos_pedidos_dir=raw_movimientos_pedidos_dir,
        cleanup_movimientos_notebook=cleanup_movimientos_notebook,
        cleanup_pedidos_notebook=cleanup_pedidos_notebook,
        cleanup_general_script=cleanup_general_script,
        download_movimientos_script=download_movimientos_script,
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
