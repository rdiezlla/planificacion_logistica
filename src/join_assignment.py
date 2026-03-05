from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)


@dataclass
class JoinOutputs:
    movements_joined: pd.DataFrame
    join_kpis: pd.DataFrame
    lead_time_summary: pd.DataFrame


def _assign_group(
    mov_group: pd.DataFrame,
    service_group: pd.DataFrame,
    window_days: int,
) -> pd.DataFrame:
    mg = mov_group.sort_values("fecha_inicio_dia").copy()
    sg = service_group.sort_values("fecha_servicio").copy()

    service_days = sg["fecha_servicio"].view("int64").to_numpy()
    service_ids = sg["service_id"].to_numpy()
    urg = sg["urgencia_norm"].to_numpy()
    typ = sg["tipo_servicio_final"].to_numpy()
    svc_date = sg["fecha_servicio"].to_numpy()

    mov_days = mg["fecha_inicio_dia"].view("int64").to_numpy()
    pos = np.searchsorted(service_days, mov_days)

    best_idx = np.full(len(mg), -1, dtype=int)
    best_abs_diff = np.full(len(mg), np.iinfo(np.int64).max, dtype=np.int64)

    for cand in (pos - 1, pos):
        valid = (cand >= 0) & (cand < len(service_days))
        cand_idx = np.where(valid, cand, 0)
        diffs = np.abs(mov_days - service_days[cand_idx])
        improve = valid & (diffs < best_abs_diff)
        best_abs_diff = np.where(improve, diffs, best_abs_diff)
        best_idx = np.where(improve, cand_idx, best_idx)

    day_ns = 24 * 3600 * 10**9
    within = (best_idx >= 0) & (best_abs_diff <= window_days * day_ns)

    mg["assigned_service_id"] = np.where(within, service_ids[np.clip(best_idx, 0, len(service_ids) - 1)], None)
    mg["assigned_fecha_servicio"] = np.where(within, svc_date[np.clip(best_idx, 0, len(service_ids) - 1)], np.datetime64("NaT"))
    mg["assigned_urgencia_norm"] = np.where(within, urg[np.clip(best_idx, 0, len(service_ids) - 1)], None)
    mg["assigned_tipo_servicio"] = np.where(within, typ[np.clip(best_idx, 0, len(service_ids) - 1)], None)

    signed_diff = np.full(len(mg), np.nan)
    valid_idx = np.where(within)[0]
    if len(valid_idx) > 0:
        signed_ns = mov_days[valid_idx] - service_days[best_idx[valid_idx]]
        signed_diff[valid_idx] = signed_ns / day_ns
    mg["delta_days_mov_minus_service"] = signed_diff
    mg["is_assigned"] = within.astype(int)
    return mg


def assign_movements_to_services(
    movements: pd.DataFrame,
    services: pd.DataFrame,
    window_days: int = 30,
) -> JoinOutputs:
    svc = (
        services[["service_id", "codigo_norm", "fecha_servicio", "urgencia_norm", "tipo_servicio_final"]]
        .dropna(subset=["codigo_norm", "fecha_servicio"])
        .drop_duplicates()
        .copy()
    )
    mov = movements.copy()

    with_code = mov[mov["pedido_externo_norm"].notna()].copy()
    without_code = mov[mov["pedido_externo_norm"].isna()].copy()

    outputs = []
    matched_codes = 0
    for code, mg in with_code.groupby("pedido_externo_norm", sort=False):
        sg = svc[svc["codigo_norm"] == code]
        if sg.empty:
            mg = mg.copy()
            mg["assigned_service_id"] = None
            mg["assigned_fecha_servicio"] = pd.NaT
            mg["assigned_urgencia_norm"] = None
            mg["assigned_tipo_servicio"] = None
            mg["delta_days_mov_minus_service"] = np.nan
            mg["is_assigned"] = 0
            outputs.append(mg)
            continue
        matched_codes += 1
        outputs.append(_assign_group(mg, sg, window_days=window_days))

    if outputs:
        joined_with_code = pd.concat(outputs, ignore_index=True)
    else:
        joined_with_code = with_code.copy()
        joined_with_code["assigned_service_id"] = None
        joined_with_code["assigned_fecha_servicio"] = pd.NaT
        joined_with_code["assigned_urgencia_norm"] = None
        joined_with_code["assigned_tipo_servicio"] = None
        joined_with_code["delta_days_mov_minus_service"] = np.nan
        joined_with_code["is_assigned"] = 0

    if not without_code.empty:
        wc = without_code.copy()
        wc["assigned_service_id"] = None
        wc["assigned_fecha_servicio"] = pd.NaT
        wc["assigned_urgencia_norm"] = None
        wc["assigned_tipo_servicio"] = None
        wc["delta_days_mov_minus_service"] = np.nan
        wc["is_assigned"] = 0
        joined = pd.concat([joined_with_code, wc], ignore_index=True)
    else:
        joined = joined_with_code

    joined["workload_atribuible"] = (
        joined["pedido_externo_norm"].notna() & joined["is_assigned"].eq(1)
    ).astype(int)
    joined["workload_no_atribuible"] = 1 - joined["workload_atribuible"]

    services_with_mov = joined.loc[joined["is_assigned"].eq(1), "assigned_service_id"].dropna().nunique()

    join_kpis = pd.DataFrame(
        [
            {
                "total_movimientos": len(joined),
                "movimientos_con_pedido_externo": int(joined["pedido_externo_norm"].notna().sum()),
                "movimientos_sin_pedido_externo": int(joined["pedido_externo_norm"].isna().sum()),
                "movimientos_asignados": int(joined["is_assigned"].sum()),
                "coverage_join_overall": joined["is_assigned"].mean(),
                "coverage_join_con_codigo": joined.loc[
                    joined["pedido_externo_norm"].notna(), "is_assigned"
                ].mean()
                if joined["pedido_externo_norm"].notna().any()
                else 0.0,
                "codigos_movimiento_con_match": matched_codes,
                "service_ids_total": int(services["service_id"].nunique()),
                "service_ids_con_movimientos": int(services_with_mov),
                "coverage_service_ids_with_mov": (
                    float(services_with_mov) / float(services["service_id"].nunique())
                    if services["service_id"].nunique() > 0
                    else 0.0
                ),
            }
        ]
    )

    lead_time_summary = compute_lead_time_summary(joined)

    LOGGER.info(
        "Join movimientos->servicios: asignados=%d/%d (%.2f%%)",
        int(joined["is_assigned"].sum()),
        len(joined),
        100.0 * joined["is_assigned"].mean() if len(joined) else 0.0,
    )

    return JoinOutputs(movements_joined=joined, join_kpis=join_kpis, lead_time_summary=lead_time_summary)


def compute_lead_time_summary(joined_movements: pd.DataFrame) -> pd.DataFrame:
    assigned = joined_movements[joined_movements["is_assigned"].eq(1)].copy()
    if assigned.empty:
        return pd.DataFrame(
            columns=[
                "urgencia_norm",
                "tipo_servicio",
                "n_services",
                "lead_time_pre_median",
                "lead_time_pre_p80",
                "lead_time_post_median",
                "lead_time_post_p80",
            ]
        )

    assigned["fecha_inicio_dia"] = pd.to_datetime(assigned["fecha_inicio_dia"]).dt.normalize()
    assigned["assigned_fecha_servicio"] = pd.to_datetime(assigned["assigned_fecha_servicio"]).dt.normalize()

    def per_service(g: pd.DataFrame) -> pd.Series:
        service_day = g["assigned_fecha_servicio"].iloc[0]
        pre = g.loc[g["fecha_inicio_dia"] <= service_day, "fecha_inicio_dia"]
        post = g.loc[g["fecha_inicio_dia"] >= service_day, "fecha_inicio_dia"]

        lead_pre = np.nan
        lead_post = np.nan
        if not pre.empty:
            lead_pre = (service_day - pre.min()).days
        if not post.empty:
            lead_post = (post.min() - service_day).days

        return pd.Series(
            {
                "urgencia_norm": g["assigned_urgencia_norm"].iloc[0],
                "tipo_servicio": g["assigned_tipo_servicio"].iloc[0],
                "lead_time_pre": lead_pre,
                "lead_time_post": lead_post,
            }
        )

    service_lt = assigned.groupby("assigned_service_id", dropna=True).apply(per_service).reset_index(drop=True)

    rows = []
    for (urg, tip), grp in service_lt.groupby(["urgencia_norm", "tipo_servicio"], dropna=False):
        pre = grp["lead_time_pre"].dropna()
        post = grp["lead_time_post"].dropna()
        rows.append(
            {
                "urgencia_norm": urg if pd.notna(urg) else "DESCONOCIDA",
                "tipo_servicio": tip if pd.notna(tip) else "desconocida",
                "n_services": len(grp),
                "lead_time_pre_median": float(pre.median()) if len(pre) else np.nan,
                "lead_time_pre_p80": float(pre.quantile(0.8)) if len(pre) else np.nan,
                "lead_time_post_median": float(post.median()) if len(post) else np.nan,
                "lead_time_post_p80": float(post.quantile(0.8)) if len(post) else np.nan,
            }
        )

    summary = pd.DataFrame(rows)
    overall = pd.DataFrame(
        [
            {
                "urgencia_norm": "ALL",
                "tipo_servicio": t,
                "n_services": int(g.shape[0]),
                "lead_time_pre_median": float(g["lead_time_pre"].median()),
                "lead_time_pre_p80": float(g["lead_time_pre"].quantile(0.8)),
                "lead_time_post_median": float(g["lead_time_post"].median()),
                "lead_time_post_p80": float(g["lead_time_post"].quantile(0.8)),
            }
            for t, g in service_lt.groupby("tipo_servicio")
        ]
    )
    if not overall.empty:
        summary = pd.concat([summary, overall], ignore_index=True)
    return summary
