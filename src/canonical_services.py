from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from .cleaning import normalize_urgency
from .service_classification import classify_service_type
from .service_id import build_service_id, extract_code_from_text, normalize_code

LOGGER = logging.getLogger(__name__)

CUTOVER_DATE = pd.Timestamp("2026-03-01").normalize()

CANONICAL_BASE_COLUMNS = [
    "service_id",
    "service_id_source",
    "source_system",
    "source_priority",
    "fecha_servicio",
    "fecha_pedido",
    "fecha_carga",
    "last_update_ts",
    "pedido_id",
    "cliente",
    "propietario",
    "tipo_servicio_final",
    "tipo_servicio_regla",
    "urgencia_norm",
    "service_status",
    "codigo_norm",
    "provincia_norm",
    "m3_out",
    "pales_out",
    "cajas_out",
    "m3_in",
    "pales_in",
    "cajas_in",
    "peso_facturable_out",
    "peso_facturable_in",
    "peso_facturable_total",
    "n_lineas_servicio",
    "row_hash",
    "ingestion_ts",
    "is_active",
]


@dataclass(frozen=True)
class CanonicalBuildResult:
    stg_services_legacy: pd.DataFrame
    stg_services_operational: pd.DataFrame
    fact_services_canonical: pd.DataFrame
    pipeline_services: pd.DataFrame
    source_audit: pd.DataFrame


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            out[c] = np.nan
    return out


def _to_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and np.isnan(value):
        return ""
    return str(value).strip()


def _first_non_empty(series: pd.Series, default: object = np.nan) -> object:
    for value in series:
        if pd.notna(value) and _to_text(value) != "":
            return value
    return default


def _parse_datetime(series: pd.Series, *, dayfirst: bool = True, normalize: bool = False) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=dayfirst)
    if normalize:
        parsed = parsed.dt.normalize()
    return parsed


def _coalesce_series(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    if not columns:
        return pd.Series(index=df.index, dtype="object")
    out = pd.Series(np.nan, index=df.index, dtype="object")
    for col in columns:
        if col not in df.columns:
            continue
        out = out.where(out.notna(), df[col])
    return out


def _stable_row_hash(parts: list[object]) -> str:
    payload = "|".join(_to_text(x) for x in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _normalize_tipo_servicio(value: object) -> str:
    txt = _to_text(value).lower()
    return txt if txt else "desconocida"


def _is_delivery_component(value: object) -> bool:
    tipo = _normalize_tipo_servicio(value)
    return tipo.startswith("entrega") or tipo == "mixto"


def _is_pickup_component(value: object) -> bool:
    tipo = _normalize_tipo_servicio(value)
    return tipo.startswith("recogida") or tipo == "mixto"


def _normalize_status(value: object) -> str:
    text = _to_text(value).upper()
    if not text:
        return "UNKNOWN"
    return text.replace(" ", "_")


def _urgency_from_operational_text(value: object) -> str:
    txt = _to_text(value).upper()
    if "MUY" in txt and "URG" in txt:
        return "MUY_URGENTE"
    if "URG" in txt:
        return "SI"
    return "DESCONOCIDA"


def prepare_legacy_staging(albaranes_clean: pd.DataFrame, ingestion_ts: pd.Timestamp) -> pd.DataFrame:
    legacy = albaranes_clean.copy()
    if legacy.empty:
        return _ensure_columns(pd.DataFrame(), CANONICAL_BASE_COLUMNS)

    legacy["fecha_servicio"] = _parse_datetime(legacy["fecha_servicio"], dayfirst=True, normalize=True)
    legacy["service_id_source"] = legacy.get("codigo_norm", legacy.get("codigo_norm_alb")).map(normalize_code)
    legacy["service_id"] = legacy.get("service_id")
    missing_sid = legacy["service_id"].isna() | legacy["service_id"].astype(str).str.strip().eq("")
    if missing_sid.any():
        legacy.loc[missing_sid, "service_id"] = build_service_id(
            legacy.loc[missing_sid, "service_id_source"],
            legacy.loc[missing_sid, "fecha_servicio"],
        )
    legacy["pedido_id"] = legacy["service_id_source"]
    legacy["cliente"] = _coalesce_series(legacy, ["solicitante_mahou", "cliente"])
    legacy["propietario"] = _coalesce_series(legacy, ["dpto_mahou", "departamento"])
    legacy["tipo_servicio_final"] = legacy.get("tipo_servicio_final", "desconocida").map(_normalize_tipo_servicio)
    legacy["tipo_servicio_regla"] = legacy.get("tipo_servicio_regla", "legacy")
    legacy["urgencia_norm"] = legacy.get("urgencia_norm", legacy.get("urgencia")).map(normalize_urgency)
    legacy["service_status"] = "LEGACY"
    legacy["codigo_norm"] = legacy["service_id_source"]
    legacy["provincia_norm"] = _coalesce_series(legacy, ["provincia_norm", "provincia_destino"])
    legacy["source_system"] = "legacy"
    legacy["source_priority"] = 10
    legacy["fecha_pedido"] = pd.NaT
    legacy["fecha_carga"] = pd.NaT
    legacy["last_update_ts"] = pd.NaT
    legacy["is_active"] = 1
    legacy["ingestion_ts"] = ingestion_ts
    legacy["n_lineas_servicio"] = 1

    numeric_cols = [
        "m3_out",
        "pales_out",
        "cajas_out",
        "m3_in",
        "pales_in",
        "cajas_in",
        "peso_facturable_out",
        "peso_facturable_in",
        "peso_facturable_total",
    ]
    for c in numeric_cols:
        legacy[c] = pd.to_numeric(legacy.get(c), errors="coerce").fillna(0.0)

    agg = (
        legacy.groupby(["service_id"], dropna=False)
        .agg(
            service_id_source=("service_id_source", _first_non_empty),
            source_system=("source_system", "first"),
            source_priority=("source_priority", "max"),
            fecha_servicio=("fecha_servicio", "min"),
            fecha_pedido=("fecha_pedido", "max"),
            fecha_carga=("fecha_carga", "max"),
            last_update_ts=("last_update_ts", "max"),
            pedido_id=("pedido_id", _first_non_empty),
            cliente=("cliente", _first_non_empty),
            propietario=("propietario", _first_non_empty),
            tipo_servicio_final=("tipo_servicio_final", _first_non_empty),
            tipo_servicio_regla=("tipo_servicio_regla", _first_non_empty),
            urgencia_norm=("urgencia_norm", _first_non_empty),
            service_status=("service_status", _first_non_empty),
            codigo_norm=("codigo_norm", _first_non_empty),
            provincia_norm=("provincia_norm", _first_non_empty),
            m3_out=("m3_out", "sum"),
            pales_out=("pales_out", "sum"),
            cajas_out=("cajas_out", "sum"),
            m3_in=("m3_in", "sum"),
            pales_in=("pales_in", "sum"),
            cajas_in=("cajas_in", "sum"),
            peso_facturable_out=("peso_facturable_out", "sum"),
            peso_facturable_in=("peso_facturable_in", "sum"),
            peso_facturable_total=("peso_facturable_total", "sum"),
            n_lineas_servicio=("n_lineas_servicio", "sum"),
            ingestion_ts=("ingestion_ts", "max"),
            is_active=("is_active", "max"),
        )
        .reset_index()
    )
    agg["row_hash"] = agg.apply(
        lambda r: _stable_row_hash(
            [
                r["service_id"],
                r["service_id_source"],
                r["fecha_servicio"],
                r["tipo_servicio_final"],
                r["urgencia_norm"],
                r["m3_out"],
                r["cajas_out"],
                r["m3_in"],
                r["cajas_in"],
                r["n_lineas_servicio"],
            ]
        ),
        axis=1,
    )
    return _ensure_columns(agg, CANONICAL_BASE_COLUMNS)[CANONICAL_BASE_COLUMNS]


def prepare_operational_staging(operational_raw: pd.DataFrame, ingestion_ts: pd.Timestamp) -> pd.DataFrame:
    op = operational_raw.copy()
    if op.empty:
        return _ensure_columns(pd.DataFrame(), CANONICAL_BASE_COLUMNS)

    op["fecha_servicio"] = _parse_datetime(
        _coalesce_series(op, ["reservation_start_date", "inicio_evento", "fin_evento"]),
        dayfirst=True,
        normalize=True,
    )
    op["fecha_pedido"] = _parse_datetime(
        _coalesce_series(op, ["creacion_pedido", "creacion_solicitud"]),
        dayfirst=True,
        normalize=False,
    )
    op["last_update_ts"] = _parse_datetime(
        _coalesce_series(op, ["ultima_modificacion", "modificacion_linea"]),
        dayfirst=True,
        normalize=False,
    )
    op["fecha_carga"] = op["last_update_ts"]

    codigo_from_pedido = op.get("pedido", pd.Series(index=op.index, dtype="object")).map(normalize_code)
    codigo_generico = op.get("codigo_generico", pd.Series(index=op.index, dtype="object")).map(normalize_code)
    codigo_from_text = _coalesce_series(op, ["solicitud"]).map(extract_code_from_text).map(normalize_code)
    op["service_id_source"] = codigo_generico.where(codigo_generico.notna(), codigo_from_pedido)
    op["service_id_source"] = op["service_id_source"].where(op["service_id_source"].notna(), codigo_from_text)

    op["service_id"] = build_service_id(op["service_id_source"], op["fecha_servicio"])
    missing_sid = op["service_id"].isna() | op["service_id"].astype(str).str.contains("MISSING_", regex=False)
    if missing_sid.any():
        op.loc[missing_sid, "service_id"] = op.loc[missing_sid].apply(
            lambda r: "OP_" + _stable_row_hash(
                [
                    r.get("id"),
                    r.get("pedido"),
                    r.get("service_id_source"),
                    r.get("fecha_servicio"),
                    r.get("articulo"),
                ]
            )[:20],
            axis=1,
        )

    type_frame = op.apply(
        lambda r: classify_service_type(
            r.get("service_id_source"),
            r.get("solicitud"),
            np.nan,
            np.nan,
        ),
        axis=1,
        result_type="expand",
    )
    op["tipo_servicio_final"] = type_frame[0].map(_normalize_tipo_servicio)
    op["tipo_servicio_regla"] = type_frame[1]
    op["urgencia_norm"] = _coalesce_series(op, ["solicitud"]).map(_urgency_from_operational_text)

    qty = pd.to_numeric(
        _coalesce_series(op, ["cant_confirmada", "cant_solicitada"]),
        errors="coerce",
    ).fillna(0.0)
    delivery_mask = op["tipo_servicio_final"].map(_is_delivery_component)
    pickup_mask = op["tipo_servicio_final"].map(_is_pickup_component)

    op["m3_out"] = 0.0
    op["pales_out"] = 0.0
    op["cajas_out"] = np.where(delivery_mask, qty, 0.0)
    op["m3_in"] = 0.0
    op["pales_in"] = 0.0
    op["cajas_in"] = np.where(pickup_mask, qty, 0.0)
    op["peso_facturable_out"] = 0.0
    op["peso_facturable_in"] = 0.0
    op["peso_facturable_total"] = 0.0
    op["n_lineas_servicio"] = 1

    op["pedido_id"] = _coalesce_series(op, ["pedido", "service_id_source"]).astype(str).str.strip()
    op["cliente"] = _coalesce_series(op, ["peticionario", "localizacion", "nombre"])
    op["propietario"] = _coalesce_series(op, ["departamento", "departamento_solicitante", "propietario"])
    op["service_status"] = _coalesce_series(op, ["estado", "estado_linea"]).map(_normalize_status)
    op["codigo_norm"] = op["service_id_source"]
    op["provincia_norm"] = _coalesce_series(op, ["provincia"])
    op["source_system"] = "operational"
    op["source_priority"] = 20
    op["ingestion_ts"] = ingestion_ts

    delete_mask = _coalesce_series(op, ["borrado_linea", "borrado_solicitud"]).notna()
    baja_mask = _coalesce_series(op, ["alta_baja"]).astype(str).str.upper().str.strip().eq("BA")
    cancelled_mask = op["service_status"].isin({"CANCELLED", "CANCELED", "ANULADO", "DELETED"})
    op["is_active"] = (~delete_mask & ~baja_mask & ~cancelled_mask).astype(int)

    op = op[op["fecha_servicio"].notna()].copy()
    if op.empty:
        return _ensure_columns(pd.DataFrame(), CANONICAL_BASE_COLUMNS)

    agg = (
        op.groupby(["service_id"], dropna=False)
        .agg(
            service_id_source=("service_id_source", _first_non_empty),
            source_system=("source_system", "first"),
            source_priority=("source_priority", "max"),
            fecha_servicio=("fecha_servicio", "min"),
            fecha_pedido=("fecha_pedido", "min"),
            fecha_carga=("fecha_carga", "max"),
            last_update_ts=("last_update_ts", "max"),
            pedido_id=("pedido_id", _first_non_empty),
            cliente=("cliente", _first_non_empty),
            propietario=("propietario", _first_non_empty),
            tipo_servicio_final=("tipo_servicio_final", _first_non_empty),
            tipo_servicio_regla=("tipo_servicio_regla", _first_non_empty),
            urgencia_norm=("urgencia_norm", _first_non_empty),
            service_status=("service_status", _first_non_empty),
            codigo_norm=("codigo_norm", _first_non_empty),
            provincia_norm=("provincia_norm", _first_non_empty),
            m3_out=("m3_out", "sum"),
            pales_out=("pales_out", "sum"),
            cajas_out=("cajas_out", "sum"),
            m3_in=("m3_in", "sum"),
            pales_in=("pales_in", "sum"),
            cajas_in=("cajas_in", "sum"),
            peso_facturable_out=("peso_facturable_out", "sum"),
            peso_facturable_in=("peso_facturable_in", "sum"),
            peso_facturable_total=("peso_facturable_total", "sum"),
            n_lineas_servicio=("n_lineas_servicio", "sum"),
            ingestion_ts=("ingestion_ts", "max"),
            is_active=("is_active", "max"),
        )
        .reset_index()
    )
    agg["row_hash"] = agg.apply(
        lambda r: _stable_row_hash(
            [
                r["service_id"],
                r["service_id_source"],
                r["fecha_servicio"],
                r["tipo_servicio_final"],
                r["service_status"],
                r["cajas_out"],
                r["cajas_in"],
                r["n_lineas_servicio"],
            ]
        ),
        axis=1,
    )
    return _ensure_columns(agg, CANONICAL_BASE_COLUMNS)[CANONICAL_BASE_COLUMNS]


def _compute_match_key(df: pd.DataFrame) -> pd.Series:
    fecha_str = pd.to_datetime(df["fecha_servicio"], errors="coerce").dt.strftime("%Y-%m-%d")
    source_id = df["service_id_source"].astype("string").fillna("")
    sid = df["service_id"].astype("string").fillna("")
    key_base = np.where(source_id.str.strip().ne(""), source_id, sid)
    return pd.Series(key_base, index=df.index).astype(str) + "__" + fecha_str.fillna("MISSING_DATE")


def _select_row_by_mode(
    grp: pd.DataFrame,
    *,
    selection_mode: str,
    cutover_date: pd.Timestamp,
) -> tuple[pd.Series, int]:
    g = grp.sort_values(["source_priority", "last_update_ts", "ingestion_ts"]).copy()
    legacy_rows = g[g["source_system"].eq("legacy")]
    op_rows = g[g["source_system"].eq("operational")]
    service_day = pd.to_datetime(g["fecha_servicio"], errors="coerce").min()
    service_day = pd.Timestamp(service_day).normalize() if pd.notna(service_day) else pd.NaT

    fallback_flag = 0
    if selection_mode == "legacy":
        chosen = legacy_rows.iloc[-1] if not legacy_rows.empty else g.iloc[-1]
    elif selection_mode == "operational-first":
        chosen = op_rows.iloc[-1] if not op_rows.empty else g.iloc[-1]
    else:
        if pd.notna(service_day) and service_day < cutover_date:
            chosen = legacy_rows.iloc[-1] if not legacy_rows.empty else g.iloc[-1]
        else:
            if not op_rows.empty:
                chosen = op_rows.iloc[-1]
            else:
                chosen = legacy_rows.iloc[-1] if not legacy_rows.empty else g.iloc[-1]
                if chosen["source_system"] == "legacy":
                    fallback_flag = 1

    if (
        chosen["source_system"] == "legacy"
        and pd.notna(service_day)
        and service_day >= cutover_date
        and op_rows.empty
    ):
        fallback_flag = 1

    return chosen, fallback_flag


def build_services_canonical(
    stg_services_legacy: pd.DataFrame,
    stg_services_operational: pd.DataFrame,
    *,
    selection_mode: str = "hybrid",
    cutover_date: pd.Timestamp = CUTOVER_DATE,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    allowed_modes = {"legacy", "hybrid", "operational-first"}
    if selection_mode not in allowed_modes:
        raise ValueError(f"selection_mode invalido: {selection_mode}. Esperado: {sorted(allowed_modes)}")

    legacy = _ensure_columns(stg_services_legacy, CANONICAL_BASE_COLUMNS).copy()
    operational = _ensure_columns(stg_services_operational, CANONICAL_BASE_COLUMNS).copy()
    union_raw = pd.concat([legacy, operational], ignore_index=True)
    if union_raw.empty:
        return pd.DataFrame(columns=CANONICAL_BASE_COLUMNS), pd.DataFrame(
            columns=[
                "periodo",
                "source_system",
                "n_registros",
                "n_service_id_unicos",
                "n_conflictos",
                "n_duplicados_detectados",
                "n_fallback_post_cutover",
            ]
        )

    union_raw["fecha_servicio"] = _parse_datetime(union_raw["fecha_servicio"], dayfirst=True, normalize=True)
    union_raw = union_raw[union_raw["fecha_servicio"].notna()].copy()
    union_raw["match_key"] = _compute_match_key(union_raw)
    union_raw["periodo"] = union_raw["fecha_servicio"].dt.to_period("M").astype(str)
    union_raw["duplicate_flag"] = union_raw.duplicated(["source_system", "match_key"], keep=False).astype(int)

    dedup = (
        union_raw.sort_values(["source_priority", "last_update_ts", "ingestion_ts"])
        .drop_duplicates(subset=["source_system", "match_key"], keep="last")
        .copy()
    )
    conflict_keys = set(
        dedup.groupby("match_key", dropna=False)["source_system"].nunique().loc[lambda s: s > 1].index.tolist()
    )

    selected_rows: list[pd.Series] = []
    for _, grp in dedup.groupby("match_key", dropna=False):
        chosen, fallback_flag = _select_row_by_mode(
            grp,
            selection_mode=selection_mode,
            cutover_date=cutover_date,
        )
        chosen = chosen.copy()
        chosen["n_conflict_flag"] = int(chosen["match_key"] in conflict_keys)
        chosen["fallback_post_cutover_flag"] = int(fallback_flag)
        if chosen["fallback_post_cutover_flag"] == 1:
            chosen["source_system"] = "legacy_fallback_post_cutover"
            chosen["source_priority"] = 15
        selected_rows.append(chosen)

    canonical = pd.DataFrame(selected_rows).reset_index(drop=True)
    if canonical.empty:
        return pd.DataFrame(columns=CANONICAL_BASE_COLUMNS), pd.DataFrame()

    canonical["row_hash"] = canonical.apply(
        lambda r: _stable_row_hash(
            [
                r["service_id"],
                r["service_id_source"],
                r["source_system"],
                r["fecha_servicio"],
                r["tipo_servicio_final"],
                r["urgencia_norm"],
                r["m3_out"],
                r["cajas_out"],
                r["m3_in"],
                r["cajas_in"],
                r["n_lineas_servicio"],
            ]
        ),
        axis=1,
    )
    canonical["periodo"] = canonical["fecha_servicio"].dt.to_period("M").astype(str)

    base_audit = (
        canonical.groupby(["periodo", "source_system"], dropna=False)
        .agg(
            n_registros=("service_id", "count"),
            n_service_id_unicos=("service_id", "nunique"),
            n_conflictos=("n_conflict_flag", "sum"),
            n_fallback_post_cutover=("fallback_post_cutover_flag", "sum"),
        )
        .reset_index()
    )
    dup_audit = (
        union_raw.groupby(["periodo", "source_system"], dropna=False)["duplicate_flag"]
        .sum()
        .reset_index(name="n_duplicados_detectados")
    )
    audit = base_audit.merge(dup_audit, on=["periodo", "source_system"], how="left")
    audit["n_duplicados_detectados"] = pd.to_numeric(audit["n_duplicados_detectados"], errors="coerce").fillna(0).astype(int)
    for c in ["n_registros", "n_service_id_unicos", "n_conflictos", "n_fallback_post_cutover"]:
        audit[c] = pd.to_numeric(audit[c], errors="coerce").fillna(0).astype(int)
    audit = audit.sort_values(["periodo", "source_system"]).reset_index(drop=True)
    audit = audit[
        [
            "periodo",
            "source_system",
            "n_registros",
            "n_service_id_unicos",
            "n_conflictos",
            "n_duplicados_detectados",
            "n_fallback_post_cutover",
        ]
    ]

    canonical = canonical.sort_values(["fecha_servicio", "service_id"]).reset_index(drop=True)
    return _ensure_columns(canonical, CANONICAL_BASE_COLUMNS + ["periodo", "n_conflict_flag", "fallback_post_cutover_flag"]), audit


def canonical_to_pipeline_services(canonical_df: pd.DataFrame, cutoff: pd.Timestamp) -> pd.DataFrame:
    if canonical_df.empty:
        return pd.DataFrame(
            columns=[
                "service_id",
                "codigo_norm_alb",
                "codigo_norm",
                "fecha_servicio",
                "tipo_servicio_final",
                "tipo_servicio_regla",
                "urgencia_norm",
                "provincia_norm",
                "m3_out",
                "cajas_out",
                "pales_out",
                "peso_facturable_out",
                "m3_in",
                "cajas_in",
                "pales_in",
                "peso_facturable_in",
                "peso_facturable_total",
                "n_lineas_servicio",
                "is_historical",
                "source_system",
                "service_status",
            ]
        )

    svc = canonical_df.copy()
    svc = svc[svc["is_active"].fillna(0).astype(int).eq(1)].copy()
    svc["fecha_servicio"] = _parse_datetime(svc["fecha_servicio"], dayfirst=True, normalize=True)
    svc["codigo_norm"] = svc["codigo_norm"].where(svc["codigo_norm"].notna(), svc["service_id_source"])
    svc["codigo_norm_alb"] = svc["codigo_norm"]
    svc["tipo_servicio_final"] = svc["tipo_servicio_final"].map(_normalize_tipo_servicio)
    svc["tipo_servicio_regla"] = svc["tipo_servicio_regla"].fillna("canonical_source")
    svc["urgencia_norm"] = svc["urgencia_norm"].fillna("DESCONOCIDA").map(normalize_urgency)
    svc["is_historical"] = (svc["fecha_servicio"] <= pd.Timestamp(cutoff).normalize()).astype(int)
    numeric_cols = [
        "m3_out",
        "cajas_out",
        "pales_out",
        "peso_facturable_out",
        "m3_in",
        "cajas_in",
        "pales_in",
        "peso_facturable_in",
        "peso_facturable_total",
        "n_lineas_servicio",
    ]
    for c in numeric_cols:
        svc[c] = pd.to_numeric(svc[c], errors="coerce").fillna(0.0)
    cols = [
        "service_id",
        "codigo_norm_alb",
        "codigo_norm",
        "fecha_servicio",
        "tipo_servicio_final",
        "tipo_servicio_regla",
        "urgencia_norm",
        "provincia_norm",
        "m3_out",
        "cajas_out",
        "pales_out",
        "peso_facturable_out",
        "m3_in",
        "cajas_in",
        "pales_in",
        "peso_facturable_in",
        "peso_facturable_total",
        "n_lineas_servicio",
        "is_historical",
        "source_system",
        "service_status",
    ]
    return _ensure_columns(svc, cols)[cols]


def build_canonical_layer(
    albaranes_clean: pd.DataFrame,
    operational_raw: pd.DataFrame | None,
    *,
    selection_mode: str,
    cutoff: pd.Timestamp,
    cutover_date: pd.Timestamp = CUTOVER_DATE,
) -> CanonicalBuildResult:
    ingestion_ts = pd.Timestamp.utcnow().tz_localize(None)
    stg_legacy = prepare_legacy_staging(albaranes_clean, ingestion_ts=ingestion_ts)
    stg_operational = (
        prepare_operational_staging(operational_raw, ingestion_ts=ingestion_ts)
        if operational_raw is not None and not operational_raw.empty
        else _ensure_columns(pd.DataFrame(), CANONICAL_BASE_COLUMNS)
    )
    canonical, audit = build_services_canonical(
        stg_legacy,
        stg_operational,
        selection_mode=selection_mode,
        cutover_date=pd.Timestamp(cutover_date).normalize(),
    )

    pipeline_services = canonical_to_pipeline_services(canonical, cutoff=cutoff)
    LOGGER.info(
        "Capa canonica servicios: legacy=%d | operational=%d | canonical=%d | modo=%s | cutover=%s",
        len(stg_legacy),
        len(stg_operational),
        len(pipeline_services),
        selection_mode,
        pd.Timestamp(cutover_date).date(),
    )
    return CanonicalBuildResult(
        stg_services_legacy=stg_legacy,
        stg_services_operational=stg_operational,
        fact_services_canonical=canonical,
        pipeline_services=pipeline_services,
        source_audit=audit,
    )
