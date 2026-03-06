from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .service_id import build_service_id, extract_code_year

LOGGER = logging.getLogger(__name__)


@dataclass
class JoinOutputs:
    movements_joined: pd.DataFrame
    join_kpis: pd.DataFrame
    lead_time_summary: pd.DataFrame


def _first_non_empty(series: pd.Series, default: str) -> str:
    for value in series:
        if pd.notna(value) and str(value).strip() != "":
            return str(value)
    return default


def _consolidate_services_for_join(services: pd.DataFrame) -> pd.DataFrame:
    svc = services.copy()
    if "codigo_norm_alb" not in svc.columns and "codigo_norm" in svc.columns:
        svc["codigo_norm_alb"] = svc["codigo_norm"]

    svc["fecha_servicio"] = pd.to_datetime(svc["fecha_servicio"], errors="coerce").dt.normalize()
    svc = svc.dropna(subset=["codigo_norm_alb", "fecha_servicio"]).copy()

    if svc.empty:
        return pd.DataFrame(
            columns=[
                "service_id",
                "codigo_norm_alb",
                "codigo_norm",
                "fecha_servicio",
                "urgencia_norm",
                "tipo_servicio_final",
            ]
        )

    numeric_metric_cols = [
        c
        for c in svc.select_dtypes(include=[np.number]).columns
        if c not in {"is_historical"}
    ]
    agg_map: dict[str, object] = {
        "urgencia_norm": lambda s: _first_non_empty(s, "DESCONOCIDA"),
        "tipo_servicio_final": lambda s: _first_non_empty(s, "desconocida"),
    }
    for col in numeric_metric_cols:
        agg_map[col] = "sum"

    consolidated = (
        svc.groupby(["codigo_norm_alb", "fecha_servicio"], dropna=False)
        .agg(agg_map)
        .reset_index()
    )
    consolidated["service_id"] = build_service_id(
        consolidated["codigo_norm_alb"], consolidated["fecha_servicio"]
    )
    consolidated["codigo_norm"] = consolidated["codigo_norm_alb"]
    return consolidated


def _empty_assignment_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["assigned_service_id"] = None
    out["assigned_fecha_servicio"] = pd.NaT
    out["assigned_urgencia_norm"] = None
    out["assigned_tipo_servicio"] = None
    out["delta_days_mov_minus_service"] = np.nan
    out["is_assigned"] = 0
    out["assigned_via"] = None
    return out


def _assign_group(
    mov_group: pd.DataFrame,
    service_group: pd.DataFrame,
    window_days: int,
) -> pd.DataFrame:
    mg = mov_group.copy()
    sg = service_group.sort_values("fecha_servicio").copy()

    if sg.empty:
        return _empty_assignment_columns(mg)

    # If code has a single service date, assign directly by code.
    if sg["fecha_servicio"].nunique() == 1:
        selected = sg.iloc[0]
        mg["assigned_service_id"] = selected["service_id"]
        mg["assigned_fecha_servicio"] = selected["fecha_servicio"]
        mg["assigned_urgencia_norm"] = selected.get("urgencia_norm")
        mg["assigned_tipo_servicio"] = selected.get("tipo_servicio_final")
        mg["is_assigned"] = 1
        mg["assigned_via"] = "single_date"

        mov_day = pd.to_datetime(mg["fecha_inicio"], errors="coerce")
        mg["delta_days_mov_minus_service"] = (
            (mov_day - pd.Timestamp(selected["fecha_servicio"])) / pd.Timedelta(days=1)
        )
        return mg

    mg = mg.sort_values("fecha_inicio").copy()
    mg_valid = mg[mg["fecha_inicio"].notna()].copy()
    mg_invalid = mg[mg["fecha_inicio"].isna()].copy()

    if mg_valid.empty:
        return _empty_assignment_columns(mg)

    service_days = sg["fecha_servicio"].astype("int64").to_numpy()
    service_ids = sg["service_id"].to_numpy()
    urg = sg["urgencia_norm"].to_numpy()
    typ = sg["tipo_servicio_final"].to_numpy()
    svc_date = sg["fecha_servicio"].to_numpy()

    mov_days = mg_valid["fecha_inicio"].astype("int64").to_numpy()
    pos = np.searchsorted(service_days, mov_days)

    best_idx = np.full(len(mg_valid), -1, dtype=int)
    best_abs_diff = np.full(len(mg_valid), np.iinfo(np.int64).max, dtype=np.int64)

    for cand in (pos - 1, pos):
        valid = (cand >= 0) & (cand < len(service_days))
        cand_idx = np.where(valid, cand, 0)
        diffs = np.abs(mov_days - service_days[cand_idx])
        improve = valid & (diffs < best_abs_diff)
        best_abs_diff = np.where(improve, diffs, best_abs_diff)
        best_idx = np.where(improve, cand_idx, best_idx)

    day_ns = 24 * 3600 * 10**9
    within = (best_idx >= 0) & (best_abs_diff <= window_days * day_ns)

    assigned = mg_valid.copy()
    assigned["assigned_service_id"] = np.where(
        within,
        service_ids[np.clip(best_idx, 0, len(service_ids) - 1)],
        None,
    )
    assigned["assigned_fecha_servicio"] = np.where(
        within,
        svc_date[np.clip(best_idx, 0, len(service_ids) - 1)],
        np.datetime64("NaT"),
    )
    assigned["assigned_urgencia_norm"] = np.where(
        within,
        urg[np.clip(best_idx, 0, len(service_ids) - 1)],
        None,
    )
    assigned["assigned_tipo_servicio"] = np.where(
        within,
        typ[np.clip(best_idx, 0, len(service_ids) - 1)],
        None,
    )

    signed_diff = np.full(len(assigned), np.nan)
    valid_idx = np.where(within)[0]
    if len(valid_idx) > 0:
        signed_ns = mov_days[valid_idx] - service_days[best_idx[valid_idx]]
        signed_diff[valid_idx] = signed_ns / day_ns

    assigned["delta_days_mov_minus_service"] = signed_diff
    assigned["is_assigned"] = within.astype(int)
    assigned["assigned_via"] = np.where(within, "nearest_date", None)

    if not mg_invalid.empty:
        mg_invalid = _empty_assignment_columns(mg_invalid)
        assigned = pd.concat([assigned, mg_invalid], ignore_index=True)

    return assigned


def _assign_by_code_column(
    movements: pd.DataFrame,
    services_consolidated: pd.DataFrame,
    window_days: int,
    code_col: str,
) -> pd.DataFrame:
    mov = movements.copy()
    mov[code_col] = mov[code_col].where(mov[code_col].notna(), None)

    with_code = mov[mov[code_col].notna()].copy()
    without_code = mov[mov[code_col].isna()].copy()

    outputs: list[pd.DataFrame] = []

    for code, mg in with_code.groupby(code_col, sort=False):
        sg = services_consolidated[services_consolidated["codigo_norm_alb"] == code].copy()

        code_year = extract_code_year(code)
        if code_year is not None:
            sg = sg[sg["fecha_servicio"].dt.year.eq(code_year)].copy()

        if sg.empty:
            outputs.append(_empty_assignment_columns(mg))
            continue

        outputs.append(_assign_group(mg, sg, window_days=window_days))

    if outputs:
        joined_with_code = pd.concat(outputs, ignore_index=True)
    else:
        joined_with_code = _empty_assignment_columns(with_code)

    if not without_code.empty:
        without_code = _empty_assignment_columns(without_code)
        joined = pd.concat([joined_with_code, without_code], ignore_index=True)
    else:
        joined = joined_with_code

    return joined


def assign_movements_to_services(
    movements: pd.DataFrame,
    services: pd.DataFrame,
    window_days: int = 30,
) -> JoinOutputs:
    svc = _consolidate_services_for_join(services)

    mov = movements.copy()
    mov["__row_id"] = np.arange(len(mov))
    mov["fecha_inicio"] = pd.to_datetime(mov["fecha_inicio"], errors="coerce")
    mov["fecha_inicio_dia"] = mov["fecha_inicio"].dt.normalize()

    joined = _assign_by_code_column(
        movements=mov,
        services_consolidated=svc,
        window_days=window_days,
        code_col="codigo_norm_mov",
    )

    primary_coverage = float(joined["is_assigned"].mean()) if len(joined) else 0.0
    if primary_coverage < 0.05 and "codigo_norm_mov_regex_otro" in joined.columns:
        candidates = joined[
            joined["is_assigned"].eq(0)
            & joined["codigo_norm_mov_regex_otro"].notna()
            & (
                joined["codigo_norm_mov"].isna()
                | joined["codigo_norm_mov"].ne(joined["codigo_norm_mov_regex_otro"])
            )
        ].copy()

        if not candidates.empty:
            secondary = _assign_by_code_column(
                movements=candidates,
                services_consolidated=svc,
                window_days=window_days,
                code_col="codigo_norm_mov_regex_otro",
            )
            recovered = secondary[secondary["is_assigned"].eq(1)].copy()
            if not recovered.empty:
                recovered_idx = recovered.set_index("__row_id")
                joined_idx = joined.set_index("__row_id")
                cols_to_update = [
                    "assigned_service_id",
                    "assigned_fecha_servicio",
                    "assigned_urgencia_norm",
                    "assigned_tipo_servicio",
                    "delta_days_mov_minus_service",
                    "is_assigned",
                ]
                for col in cols_to_update:
                    joined_idx.loc[recovered_idx.index, col] = recovered_idx[col]
                joined_idx.loc[recovered_idx.index, "assigned_via"] = "secondary_regex"
                joined_idx.loc[recovered_idx.index, "codigo_source"] = "regex_otro_secondary"
                joined = joined_idx.reset_index()

    joined["workload_atribuible"] = joined["is_assigned"].eq(1).astype(int)
    joined["workload_no_atribuible"] = 1 - joined["workload_atribuible"]

    services_with_mov = joined.loc[
        joined["is_assigned"].eq(1), "assigned_service_id"
    ].dropna().nunique()

    codigos_alb = set(svc["codigo_norm_alb"].dropna().unique())
    codigos_mov = set(joined["codigo_norm_mov"].dropna().unique())
    n_codigos_interseccion = len(codigos_alb & codigos_mov)

    total_mov = len(joined)
    mov_con_codigo = int(joined["codigo_norm_mov"].notna().sum())
    mov_asignados = int(joined["is_assigned"].sum())
    coverage = float(mov_asignados) / float(total_mov) if total_mov else 0.0
    pct_mov_con_codigo_asignado = (
        float(mov_asignados) / float(mov_con_codigo) if mov_con_codigo else 0.0
    )

    service_ids_total = int(svc["service_id"].nunique())
    pct_service_id_con_mov = (
        float(services_with_mov) / float(service_ids_total) if service_ids_total else 0.0
    )

    join_kpis = pd.DataFrame(
        [
            {
                "total_movimientos": total_mov,
                "mov_con_codigo": mov_con_codigo,
                "mov_asignados": mov_asignados,
                "coverage": coverage,
                "n_codigos_interseccion": n_codigos_interseccion,
                "pct_mov_con_codigo_asignado": pct_mov_con_codigo_asignado,
                "pct_service_id_con_mov": pct_service_id_con_mov,
                # Backward compatibility columns.
                "movimientos_con_pedido_externo": mov_con_codigo,
                "movimientos_sin_pedido_externo": int(total_mov - mov_con_codigo),
                "movimientos_asignados": mov_asignados,
                "coverage_join_overall": coverage,
                "coverage_join_con_codigo": pct_mov_con_codigo_asignado,
                "service_ids_total": service_ids_total,
                "service_ids_con_movimientos": int(services_with_mov),
                "coverage_service_ids_with_mov": pct_service_id_con_mov,
            }
        ]
    )

    lead_time_summary = compute_lead_time_summary(joined)

    LOGGER.info(
        "Join movimientos->servicios: asignados=%d/%d (%.2f%%)",
        mov_asignados,
        total_mov,
        100.0 * coverage if total_mov else 0.0,
    )

    joined = joined.drop(columns=["__row_id"], errors="ignore")

    return JoinOutputs(
        movements_joined=joined,
        join_kpis=join_kpis,
        lead_time_summary=lead_time_summary,
    )


def compute_lead_time_summary(joined_movements: pd.DataFrame) -> pd.DataFrame:
    assigned = joined_movements[joined_movements["is_assigned"].eq(1)].copy()
    assigned = assigned[assigned["assigned_service_id"].notna()].copy()

    if assigned.empty:
        return pd.DataFrame(
            columns=[
                "urgencia",
                "tipo_servicio_final",
                "n_services",
                "lead_time_pre_median",
                "lead_time_pre_p80",
                "lead_time_pre_p95",
                "lead_time_post_median",
                "lead_time_post_p80",
                "lead_time_post_p95",
                "urgencia_norm",
                "tipo_servicio",
            ]
        )

    assigned["fecha_inicio_dia"] = pd.to_datetime(
        assigned["fecha_inicio_dia"], errors="coerce"
    ).dt.normalize()
    assigned["assigned_fecha_servicio"] = pd.to_datetime(
        assigned["assigned_fecha_servicio"], errors="coerce"
    ).dt.normalize()

    def per_service(g: pd.DataFrame) -> pd.Series:
        service_day = g["assigned_fecha_servicio"].iloc[0]
        pre = g.loc[g["fecha_inicio_dia"] <= service_day, "fecha_inicio_dia"].dropna()
        post = g.loc[g["fecha_inicio_dia"] >= service_day, "fecha_inicio_dia"].dropna()

        lead_pre = np.nan
        lead_post = np.nan
        if not pre.empty:
            lead_pre = (service_day - pre.min()).days
        if not post.empty:
            lead_post = (post.min() - service_day).days

        return pd.Series(
            {
                "urgencia": g["assigned_urgencia_norm"].iloc[0]
                if pd.notna(g["assigned_urgencia_norm"].iloc[0])
                else "DESCONOCIDA",
                "tipo_servicio_final": g["assigned_tipo_servicio"].iloc[0]
                if pd.notna(g["assigned_tipo_servicio"].iloc[0])
                else "desconocida",
                "lead_time_pre": lead_pre,
                "lead_time_post": lead_post,
            }
        )

    service_lt = (
        assigned.groupby("assigned_service_id", dropna=True)
        .apply(per_service, include_groups=False)
        .reset_index(drop=True)
    )

    rows = []
    for (urg, tip), grp in service_lt.groupby(["urgencia", "tipo_servicio_final"], dropna=False):
        pre = grp["lead_time_pre"].dropna()
        post = grp["lead_time_post"].dropna()
        rows.append(
            {
                "urgencia": urg if pd.notna(urg) else "DESCONOCIDA",
                "tipo_servicio_final": tip if pd.notna(tip) else "desconocida",
                "n_services": int(len(grp)),
                "lead_time_pre_median": float(pre.median()) if len(pre) else np.nan,
                "lead_time_pre_p80": float(pre.quantile(0.8)) if len(pre) else np.nan,
                "lead_time_pre_p95": float(pre.quantile(0.95)) if len(pre) else np.nan,
                "lead_time_post_median": float(post.median()) if len(post) else np.nan,
                "lead_time_post_p80": float(post.quantile(0.8)) if len(post) else np.nan,
                "lead_time_post_p95": float(post.quantile(0.95)) if len(post) else np.nan,
            }
        )

    summary = pd.DataFrame(rows)

    overall = []
    for tip, grp in service_lt.groupby("tipo_servicio_final", dropna=False):
        pre = grp["lead_time_pre"].dropna()
        post = grp["lead_time_post"].dropna()
        overall.append(
            {
                "urgencia": "ALL",
                "tipo_servicio_final": tip if pd.notna(tip) else "desconocida",
                "n_services": int(len(grp)),
                "lead_time_pre_median": float(pre.median()) if len(pre) else np.nan,
                "lead_time_pre_p80": float(pre.quantile(0.8)) if len(pre) else np.nan,
                "lead_time_pre_p95": float(pre.quantile(0.95)) if len(pre) else np.nan,
                "lead_time_post_median": float(post.median()) if len(post) else np.nan,
                "lead_time_post_p80": float(post.quantile(0.8)) if len(post) else np.nan,
                "lead_time_post_p95": float(post.quantile(0.95)) if len(post) else np.nan,
            }
        )

    if overall:
        summary = pd.concat([summary, pd.DataFrame(overall)], ignore_index=True)

    # Backward compatibility columns used in other modules.
    summary["urgencia_norm"] = summary["urgencia"]
    summary["tipo_servicio"] = summary["tipo_servicio_final"]
    return summary


def generate_join_debug_outputs(
    albaranes: pd.DataFrame,
    movimientos: pd.DataFrame,
    outputs_dir: Path,
) -> None:
    outputs_dir.mkdir(parents=True, exist_ok=True)

    alb = albaranes.copy()
    mov = movimientos.copy()

    alb["fecha_servicio"] = pd.to_datetime(alb.get("fecha_servicio"), errors="coerce")
    mov["fecha_inicio"] = pd.to_datetime(mov.get("fecha_inicio"), errors="coerce")

    codigos_alb = set(alb.get("codigo_norm_alb", pd.Series(dtype=object)).dropna().unique())
    codigos_mov = set(mov.get("codigo_norm_mov", pd.Series(dtype=object)).dropna().unique())
    inter = sorted(codigos_alb & codigos_mov)

    sample_alb_desc = next(
        (
            str(v)
            for v in alb.get("descripcion", pd.Series(dtype=object)).tolist()
            if pd.notna(v) and str(v).strip() != ""
        ),
        None,
    )
    sample_mov_ped = next(
        (
            str(v)
            for v in mov.get("pedido_externo", pd.Series(dtype=object)).tolist()
            if pd.notna(v) and str(v).strip() != ""
        ),
        None,
    )

    summary = pd.DataFrame(
        [
            {
                "n_albaranes_total": int(len(alb)),
                "n_albaranes_con_codigo": int(alb.get("codigo_norm_alb", pd.Series(dtype=object)).notna().sum()),
                "n_albaranes_fecha_ok": int(alb.get("fecha_servicio", pd.Series(dtype="datetime64[ns]")).notna().sum()),
                "n_mov_total": int(len(mov)),
                "n_mov_con_codigo": int(mov.get("codigo_norm_mov", pd.Series(dtype=object)).notna().sum()),
                "n_mov_fecha_ok": int(mov.get("fecha_inicio", pd.Series(dtype="datetime64[ns]")).notna().sum()),
                "n_codigos_alb_distintos": int(len(codigos_alb)),
                "n_codigos_mov_distintos": int(len(codigos_mov)),
                "n_codigos_interseccion": int(len(inter)),
                "example_intersection_code": inter[0] if inter else None,
                "sample_alb_descripcion_1": sample_alb_desc,
                "sample_mov_pedido_externo_1": sample_mov_ped,
            }
        ]
    )
    summary.to_csv(outputs_dir / "join_debug_summary.csv", index=False)

    alb_sample = (
        alb[["descripcion", "codigo_raw_alb", "codigo_norm_alb", "fecha_servicio"]]
        .head(25)
        .rename(
            columns={
                "descripcion": "original_value",
                "codigo_raw_alb": "codigo_raw",
                "codigo_norm_alb": "codigo_norm",
                "fecha_servicio": "fecha",
            }
        )
    )
    alb_sample["dataset"] = "albaranes"
    alb_sample["codigo_source"] = "descripcion"

    mov_cols = [
        "pedido_externo",
        "codigo_raw_mov",
        "codigo_norm_mov",
        "fecha_inicio",
        "codigo_source",
    ]
    mov_sample = (
        mov[mov_cols]
        .head(25)
        .rename(
            columns={
                "pedido_externo": "original_value",
                "codigo_raw_mov": "codigo_raw",
                "codigo_norm_mov": "codigo_norm",
                "fecha_inicio": "fecha",
            }
        )
    )
    mov_sample["dataset"] = "movimientos"

    samples = pd.concat([alb_sample, mov_sample], ignore_index=True)
    cols = ["dataset", "original_value", "codigo_raw", "codigo_norm", "fecha", "codigo_source"]
    samples = samples[cols]
    samples.to_csv(outputs_dir / "join_debug_samples.csv", index=False)

    alb_counts = (
        alb[alb["codigo_norm_alb"].notna()]
        .groupby("codigo_norm_alb")
        .size()
        .rename("count_albaranes")
        .reset_index()
    )
    mov_counts = (
        mov[mov["codigo_norm_mov"].notna()]
        .groupby("codigo_norm_mov")
        .size()
        .rename("count_movimientos")
        .reset_index()
    )
    inter_top = (
        alb_counts.merge(
            mov_counts,
            left_on="codigo_norm_alb",
            right_on="codigo_norm_mov",
            how="inner",
        )
        .assign(codigo_norm=lambda x: x["codigo_norm_alb"])
        [["codigo_norm", "count_albaranes", "count_movimientos"]]
        .sort_values(["count_movimientos", "count_albaranes"], ascending=[False, False])
        .head(50)
    )
    inter_top.to_csv(outputs_dir / "join_debug_intersection_top.csv", index=False)
