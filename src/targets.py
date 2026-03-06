from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


SERVICE_NUMERIC_AGGS = {
    "m3_out": "sum",
    "cajas_out": "sum",
    "pales_out": "sum",
    "peso_facturable_out": "sum",
    "m3_in": "sum",
    "cajas_in": "sum",
    "pales_in": "sum",
    "peso_facturable_in": "sum",
}


DEFAULT_URGENCIA_ORDER = ["NO", "SI", "MUY_URGENTE", "DESCONOCIDA"]
FALLBACK_LEAD_PRE_DAYS = {
    "NO": 3,
    "SI": 1,
    "MUY_URGENTE": 0,
    "DESCONOCIDA": 2,
}


def _normalize_service_type(value: object) -> str:
    if value is None:
        return "desconocida"
    text = str(value).strip().lower()
    return text if text else "desconocida"


def _is_delivery_type(value: object) -> bool:
    return _normalize_service_type(value).startswith("entrega")


def _is_pickup_type(value: object) -> bool:
    return _normalize_service_type(value).startswith("recogida")


def build_service_level(albaranes_clean: pd.DataFrame) -> pd.DataFrame:
    base = albaranes_clean.copy()
    if "codigo_norm" not in base.columns and "codigo_norm_alb" in base.columns:
        base["codigo_norm"] = base["codigo_norm_alb"]
    base = base[base["fecha_servicio"].notna()].copy()

    group_cols = [
        "service_id",
        "codigo_norm",
        "fecha_servicio",
        "tipo_servicio_final",
        "tipo_servicio_regla",
        "urgencia_norm",
        "provincia_norm",
        "is_historical",
    ]
    present_cols = [c for c in group_cols if c in base.columns]

    agg_dict = SERVICE_NUMERIC_AGGS.copy()
    agg_dict.update({"peso_facturable_total": "sum"})

    service_level = base.groupby(present_cols, dropna=False).agg(agg_dict).reset_index()
    return service_level


def _mix_ratio_by_date(service_level: pd.DataFrame) -> pd.DataFrame:
    cnt = (
        service_level.groupby(["fecha_servicio", "tipo_servicio_final"], dropna=False)["service_id"]
        .nunique()
        .reset_index(name="n")
    )
    if cnt.empty:
        return pd.DataFrame(columns=["fecha_servicio", "mix_entrega_vs_recogida"])

    cnt["is_delivery"] = cnt["tipo_servicio_final"].map(_is_delivery_type).astype(int)
    cnt["is_pickup"] = cnt["tipo_servicio_final"].map(_is_pickup_type).astype(int)

    by_day = (
        cnt.groupby("fecha_servicio", dropna=False)
        .apply(
            lambda g: pd.Series(
                {
                    "entrega": float(g.loc[g["is_delivery"].eq(1), "n"].sum()),
                    "recogida": float(g.loc[g["is_pickup"].eq(1), "n"].sum()),
                }
            ),
            include_groups=False,
        )
        .reset_index()
    )
    denom = by_day["entrega"] + by_day["recogida"]
    by_day["mix_entrega_vs_recogida"] = (by_day["entrega"] / denom).where(denom > 0)
    return by_day[["fecha_servicio", "mix_entrega_vs_recogida"]]


def build_service_targets(service_level: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily = (
        service_level.groupby(["fecha_servicio", "tipo_servicio_final"], dropna=False)
        .agg(
            conteo_servicios=("service_id", "nunique"),
            m3_out=("m3_out", "sum"),
            cajas_out=("cajas_out", "sum"),
            pales_out=("pales_out", "sum"),
            peso_facturable_out=("peso_facturable_out", "sum"),
            m3_in=("m3_in", "sum"),
            cajas_in=("cajas_in", "sum"),
            pales_in=("pales_in", "sum"),
            peso_facturable_in=("peso_facturable_in", "sum"),
            is_historical=("is_historical", "max"),
        )
        .reset_index()
        .rename(columns={"fecha_servicio": "date", "tipo_servicio_final": "tipo_servicio"})
    )

    mix = _mix_ratio_by_date(service_level).rename(columns={"fecha_servicio": "date"})
    daily = daily.merge(mix, on="date", how="left")
    daily["axis"] = "service"

    weekly = daily.copy()
    weekly["week_start"] = weekly["date"] - pd.to_timedelta(weekly["date"].dt.dayofweek, unit="D")
    weekly = (
        weekly.groupby(["week_start", "tipo_servicio"], dropna=False)
        .agg(
            conteo_servicios=("conteo_servicios", "sum"),
            m3_out=("m3_out", "sum"),
            cajas_out=("cajas_out", "sum"),
            pales_out=("pales_out", "sum"),
            peso_facturable_out=("peso_facturable_out", "sum"),
            m3_in=("m3_in", "sum"),
            cajas_in=("cajas_in", "sum"),
            pales_in=("pales_in", "sum"),
            peso_facturable_in=("peso_facturable_in", "sum"),
            is_historical=("is_historical", "max"),
        )
        .reset_index()
        .rename(columns={"week_start": "date"})
    )

    cntw = (
        daily.assign(week_start=daily["date"] - pd.to_timedelta(daily["date"].dt.dayofweek, unit="D"))
        .groupby(["week_start", "tipo_servicio"], dropna=False)["conteo_servicios"]
        .sum()
        .reset_index(name="n")
    )
    if not cntw.empty:
        cntw["is_delivery"] = cntw["tipo_servicio"].map(_is_delivery_type).astype(int)
        cntw["is_pickup"] = cntw["tipo_servicio"].map(_is_pickup_type).astype(int)
        week_mix = (
            cntw.groupby("week_start", dropna=False)
            .apply(
                lambda g: pd.Series(
                    {
                        "entrega": float(g.loc[g["is_delivery"].eq(1), "n"].sum()),
                        "recogida": float(g.loc[g["is_pickup"].eq(1), "n"].sum()),
                    }
                ),
                include_groups=False,
            )
            .reset_index()
        )
        denom = week_mix["entrega"] + week_mix["recogida"]
        week_mix["mix_entrega_vs_recogida"] = (week_mix["entrega"] / denom).where(denom > 0)
        week_mix = week_mix.rename(columns={"week_start": "date"})[["date", "mix_entrega_vs_recogida"]]
    else:
        week_mix = pd.DataFrame(columns=["date", "mix_entrega_vs_recogida"])

    weekly = weekly.merge(week_mix, on="date", how="left")
    weekly["axis"] = "service"

    return daily, weekly


def build_workload_targets(movements_joined: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    mov = movements_joined.copy()
    mov["date"] = pd.to_datetime(mov["fecha_inicio_dia"], errors="coerce").dt.normalize()
    mov = mov[mov["date"].notna()].copy()

    tipo_mov_series = mov.get("tipo_movimiento", pd.Series(index=mov.index, dtype="object"))
    assigned_tipo_series = mov.get("assigned_tipo_servicio", pd.Series(index=mov.index, dtype="object"))
    mov["tipo_movimiento_norm"] = tipo_mov_series.astype(str).str.upper().str.strip()
    mov["assigned_tipo_servicio_norm"] = assigned_tipo_series.map(_normalize_service_type)
    mov["is_picking"] = mov["tipo_movimiento_norm"].eq("PI").astype(int)
    mov["picking_movs"] = mov["is_picking"]
    mov["picking_movs_atribuibles"] = (
        mov["is_picking"].eq(1) & mov["workload_atribuible"].eq(1)
    ).astype(int)
    mov["picking_movs_atribuibles_entrega"] = (
        mov["is_picking"].eq(1)
        & mov["workload_atribuible"].eq(1)
        & mov["assigned_tipo_servicio_norm"].map(_is_delivery_type)
    ).astype(int)
    mov["picking_movs_no_atribuibles"] = (
        mov["is_picking"].eq(1) & mov["workload_no_atribuible"].eq(1)
    ).astype(int)

    base = (
        mov.groupby("date", dropna=False)
        .agg(
            conteo_movimientos=("fecha_inicio", "count"),
            unidades_movidas=("cantidad", "sum"),
            skus_distintos=("articulo", "nunique"),
            workload_atribuible_movs=("workload_atribuible", "sum"),
            workload_no_atribuible_movs=("workload_no_atribuible", "sum"),
            picking_movs=("picking_movs", "sum"),
            picking_movs_atribuibles=("picking_movs_atribuibles", "sum"),
            picking_movs_atribuibles_entrega=("picking_movs_atribuibles_entrega", "sum"),
            picking_movs_no_atribuibles=("picking_movs_no_atribuibles", "sum"),
        )
        .reset_index()
    )

    tipo_dist = (
        mov.groupby(["date", "tipo_movimiento"], dropna=False)["fecha_inicio"]
        .count()
        .reset_index(name="n")
        .pivot(index="date", columns="tipo_movimiento", values="n")
        .fillna(0)
    )
    tipo_dist.columns = [f"movtype_{str(c).lower()}" for c in tipo_dist.columns]
    tipo_dist = tipo_dist.reset_index()

    daily = base.merge(tipo_dist, on="date", how="left")
    daily["axis"] = "workload"
    daily["tipo_servicio"] = "ALL"
    daily["is_historical"] = 1

    weekly = daily.copy()
    weekly["week_start"] = weekly["date"] - pd.to_timedelta(weekly["date"].dt.dayofweek, unit="D")
    agg_cols = {c: "sum" for c in weekly.columns if c.startswith("movtype_")}
    agg_cols.update(
        {
            "conteo_movimientos": "sum",
            "unidades_movidas": "sum",
            "skus_distintos": "sum",
            "workload_atribuible_movs": "sum",
            "workload_no_atribuible_movs": "sum",
            "picking_movs": "sum",
            "picking_movs_atribuibles": "sum",
            "picking_movs_atribuibles_entrega": "sum",
            "picking_movs_no_atribuibles": "sum",
            "is_historical": "max",
        }
    )

    weekly = (
        weekly.groupby("week_start", dropna=False)
        .agg(agg_cols)
        .reset_index()
        .rename(columns={"week_start": "date"})
    )
    weekly["axis"] = "workload"
    weekly["tipo_servicio"] = "ALL"

    return daily, weekly


def _quantile80(series: pd.Series) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return np.nan
    return float(s.quantile(0.8))


def is_workday_madrid(day: pd.Timestamp, holidays: set[pd.Timestamp]) -> bool:
    day = pd.Timestamp(day).normalize()
    return day.dayofweek < 5 and day not in holidays


def _shift_to_previous_workday(day: pd.Timestamp, holidays: set[pd.Timestamp]) -> pd.Timestamp:
    d = pd.Timestamp(day).normalize()
    while not is_workday_madrid(d, holidays):
        d = d - pd.Timedelta(days=1)
    return d


def _build_service_assignment_frame(
    join_movements: pd.DataFrame,
    service_level_hist: pd.DataFrame,
) -> pd.DataFrame:
    svc = service_level_hist.copy()
    svc = svc.drop_duplicates(subset=["service_id"]).copy()
    svc["fecha_servicio"] = pd.to_datetime(svc["fecha_servicio"], errors="coerce").dt.normalize()
    svc["tipo_servicio"] = svc.get("tipo_servicio_final", "desconocida").map(_normalize_service_type)
    svc["urgencia"] = svc.get("urgencia_norm", "DESCONOCIDA").fillna("DESCONOCIDA").astype(str)
    svc = svc[
        svc["service_id"].notna()
        & svc["fecha_servicio"].notna()
        & svc["tipo_servicio"].map(_is_delivery_type)
    ].copy()
    if svc.empty:
        return pd.DataFrame(
            columns=[
                "service_id",
                "fecha_servicio",
                "tipo_servicio",
                "urgencia",
                "month",
                "dow",
                "picking_movs_atribuibles_entrega",
                "ratio_picking_por_servicio",
            ]
        )

    mov = join_movements.copy()
    tipo_mov_series = mov.get("tipo_movimiento", pd.Series(index=mov.index, dtype="object"))
    assigned_sid_series = mov.get("assigned_service_id", pd.Series(index=mov.index, dtype="object"))
    mov["tipo_movimiento_norm"] = tipo_mov_series.astype(str).str.upper().str.strip()
    delivery_ids = set(svc["service_id"].dropna().astype(str).unique())
    mov["assigned_service_id_norm"] = assigned_sid_series.astype(str)

    picking_movs = (
        mov[
            mov["is_assigned"].eq(1)
            & mov["tipo_movimiento_norm"].eq("PI")
            & mov["assigned_service_id_norm"].isin(delivery_ids)
        ]
        .groupby("assigned_service_id_norm", dropna=False)
        .size()
        .rename("picking_movs_atribuibles_entrega")
        .reset_index()
        .rename(columns={"assigned_service_id_norm": "service_id_norm"})
    )

    out = svc.copy()
    out["service_id_norm"] = out["service_id"].astype(str)
    out = out.merge(picking_movs, on="service_id_norm", how="left")
    out["picking_movs_atribuibles_entrega"] = pd.to_numeric(
        out["picking_movs_atribuibles_entrega"], errors="coerce"
    ).fillna(0.0)
    out["month"] = out["fecha_servicio"].dt.month.astype(int)
    out["dow"] = out["fecha_servicio"].dt.dayofweek.astype(int)
    out["ratio_picking_por_servicio"] = out["picking_movs_atribuibles_entrega"]

    return out[
        [
            "service_id",
            "fecha_servicio",
            "tipo_servicio",
            "urgencia",
            "month",
            "dow",
            "picking_movs_atribuibles_entrega",
            "ratio_picking_por_servicio",
        ]
    ]


def _build_ratio_tables(service_assignments: pd.DataFrame) -> dict[str, Any]:
    df = service_assignments.copy()

    def _agg(keys: list[str]) -> pd.DataFrame:
        return (
            df.groupby(keys, dropna=False)
            .agg(
                ratio_p50=("ratio_picking_por_servicio", "median"),
                ratio_p80=("ratio_picking_por_servicio", _quantile80),
            )
            .reset_index()
        )

    return {
        "month_dow": _agg(["month", "dow"]),
        "month": _agg(["month"]),
        "dow": _agg(["dow"]),
        "global": {
            "ratio_p50": float(df["ratio_picking_por_servicio"].median()) if len(df) else 0.0,
            "ratio_p80": _quantile80(df["ratio_picking_por_servicio"]) if len(df) else 0.0,
        },
    }


def _lookup_ratio(
    ratio_tables: dict[str, Any],
    month: int,
    dow: int,
    column: str,
) -> float:
    checks: list[tuple[str, dict[str, Any]]] = [
        ("month_dow", {"month": month, "dow": dow}),
        ("month", {"month": month}),
        ("dow", {"dow": dow}),
    ]

    for level, filters in checks:
        table = ratio_tables[level]
        if table.empty:
            continue
        mask = pd.Series(True, index=table.index)
        for key, val in filters.items():
            mask &= table[key].eq(val)
        if not mask.any():
            continue
        value = pd.to_numeric(table.loc[mask, column], errors="coerce").dropna()
        if not value.empty:
            return float(value.iloc[0])

    fallback = ratio_tables["global"].get(column)
    if fallback is None or pd.isna(fallback):
        return 0.0
    return float(fallback)


def _build_urgency_shares(service_assignments: pd.DataFrame) -> dict[str, Any]:
    df = service_assignments.copy()

    exact = (
        df.groupby(["tipo_servicio", "month", "dow", "urgencia"], dropna=False)["service_id"]
        .nunique()
        .reset_index(name="n")
    )
    exact["share"] = (
        exact["n"]
        / exact.groupby(["tipo_servicio", "month", "dow"], dropna=False)["n"].transform("sum").replace({0: np.nan})
    )

    by_tipo_month = (
        df.groupby(["tipo_servicio", "month", "urgencia"], dropna=False)["service_id"]
        .nunique()
        .reset_index(name="n")
    )
    by_tipo_month["share"] = (
        by_tipo_month["n"]
        / by_tipo_month.groupby(["tipo_servicio", "month"], dropna=False)["n"].transform("sum").replace({0: np.nan})
    )

    by_tipo = (
        df.groupby(["tipo_servicio", "urgencia"], dropna=False)["service_id"]
        .nunique()
        .reset_index(name="n")
    )
    by_tipo["share"] = (
        by_tipo["n"]
        / by_tipo.groupby(["tipo_servicio"], dropna=False)["n"].transform("sum").replace({0: np.nan})
    )

    global_share = (
        df.groupby("urgencia", dropna=False)["service_id"]
        .nunique()
        .reset_index(name="n")
    )
    denom_global = float(global_share["n"].sum())
    global_share["share"] = (global_share["n"] / denom_global) if denom_global > 0 else 0.0

    return {
        "exact": exact,
        "tipo_month": by_tipo_month,
        "tipo": by_tipo,
        "global": global_share,
    }


def _resolve_urgency_distribution(
    urgency_tables: dict[str, Any],
    tipo_servicio: str,
    month: int,
    dow: int,
    urgencia_levels: list[str],
) -> dict[str, float]:
    exact = urgency_tables["exact"]
    by_tipo_month = urgency_tables["tipo_month"]
    by_tipo = urgency_tables["tipo"]
    global_share = urgency_tables["global"]

    dist: dict[str, float] = {}

    sel = exact[
        exact["tipo_servicio"].eq(tipo_servicio)
        & exact["month"].eq(month)
        & exact["dow"].eq(dow)
    ]
    if sel.empty:
        sel = by_tipo_month[
            by_tipo_month["tipo_servicio"].eq(tipo_servicio)
            & by_tipo_month["month"].eq(month)
        ]
    if sel.empty:
        sel = by_tipo[by_tipo["tipo_servicio"].eq(tipo_servicio)]
    if sel.empty:
        sel = global_share.copy()

    for urg in urgencia_levels:
        s = sel.loc[sel["urgencia"].eq(urg), "share"] if "urgencia" in sel.columns else pd.Series(dtype=float)
        dist[urg] = float(s.iloc[0]) if not s.empty and pd.notna(s.iloc[0]) else 0.0

    total = float(sum(dist.values()))
    if total <= 0:
        uniform = 1.0 / max(len(urgencia_levels), 1)
        return {u: uniform for u in urgencia_levels}

    return {u: (dist[u] / total) for u in urgencia_levels}


def _build_lead_lookup(lead_time_summary: pd.DataFrame) -> tuple[dict[tuple[str, str], tuple[float, float]], dict[str, tuple[float, float]]]:
    lt = lead_time_summary.copy()
    if lt.empty:
        return {}, {}

    if "urgencia" not in lt.columns and "urgencia_norm" in lt.columns:
        lt["urgencia"] = lt["urgencia_norm"]
    if "tipo_servicio_final" not in lt.columns and "tipo_servicio" in lt.columns:
        lt["tipo_servicio_final"] = lt["tipo_servicio"]

    by_key: dict[tuple[str, str], tuple[float, float]] = {}
    by_tipo_all: dict[str, tuple[float, float]] = {}

    for _, row in lt.iterrows():
        urg = str(row.get("urgencia", "DESCONOCIDA"))
        tip = _normalize_service_type(row.get("tipo_servicio_final", "desconocida"))
        pre_med = pd.to_numeric(row.get("lead_time_pre_median"), errors="coerce")
        pre_p80 = pd.to_numeric(row.get("lead_time_pre_p80"), errors="coerce")
        by_key[(urg, tip)] = (float(pre_med) if pd.notna(pre_med) else np.nan, float(pre_p80) if pd.notna(pre_p80) else np.nan)
        if urg == "ALL":
            by_tipo_all[tip] = by_key[(urg, tip)]

    return by_key, by_tipo_all


def _resolve_lead_pre_days(
    urgencia: str,
    tipo_servicio: str,
    quantile: str,
    lead_by_key: dict[tuple[str, str], tuple[float, float]],
    lead_by_tipo_all: dict[str, tuple[float, float]],
) -> int:
    tipo_servicio = _normalize_service_type(tipo_servicio)
    val = np.nan
    if (urgencia, tipo_servicio) in lead_by_key:
        pair = lead_by_key[(urgencia, tipo_servicio)]
        val = pair[0] if quantile == "p50" else pair[1]

    if pd.isna(val) and tipo_servicio in lead_by_tipo_all:
        pair = lead_by_tipo_all[tipo_servicio]
        val = pair[0] if quantile == "p50" else pair[1]

    if pd.isna(val):
        val = float(FALLBACK_LEAD_PRE_DAYS.get(urgencia, FALLBACK_LEAD_PRE_DAYS["DESCONOCIDA"]))

    return int(max(round(float(val)), 0))


def _allocate_expected_p50_by_prep_day(
    service_events: pd.DataFrame,
    ratio_tables: dict[str, Any],
    urgency_tables: dict[str, Any],
    urgencia_levels: list[str],
    lead_by_key: dict[tuple[str, str], tuple[float, float]],
    lead_by_tipo_all: dict[str, tuple[float, float]],
    holidays: set[pd.Timestamp],
) -> dict[pd.Timestamp, float]:
    out: dict[pd.Timestamp, float] = {}
    if service_events.empty:
        return out

    for _, row in service_events.iterrows():
        service_day = pd.Timestamp(row["date"]).normalize()
        tipo = _normalize_service_type(row.get("tipo_servicio", "desconocida"))
        if not _is_delivery_type(tipo):
            continue

        eventos = float(pd.to_numeric(row.get("eventos"), errors="coerce"))
        if not np.isfinite(eventos) or eventos <= 0:
            continue

        month = int(service_day.month)
        dow = int(service_day.dayofweek)
        ratio_p50 = _lookup_ratio(ratio_tables, month=month, dow=dow, column="ratio_p50")
        expected_total = max(ratio_p50 * eventos, 0.0)
        if expected_total <= 0:
            continue

        urg_dist = _resolve_urgency_distribution(
            urgency_tables=urgency_tables,
            tipo_servicio=tipo,
            month=month,
            dow=dow,
            urgencia_levels=urgencia_levels,
        )
        for urg, share in urg_dist.items():
            if share <= 0:
                continue
            shift = _resolve_lead_pre_days(
                urgencia=urg,
                tipo_servicio=tipo,
                quantile="p50",
                lead_by_key=lead_by_key,
                lead_by_tipo_all=lead_by_tipo_all,
            )
            plan_day = _shift_to_previous_workday(service_day - pd.Timedelta(days=shift), holidays)
            out[plan_day] = out.get(plan_day, 0.0) + expected_total * share
    return out


def _build_residual_tables(
    service_assignments: pd.DataFrame,
    join_movements: pd.DataFrame,
    ratio_tables: dict[str, Any],
    urgency_tables: dict[str, Any],
    urgencia_levels: list[str],
    lead_by_key: dict[tuple[str, str], tuple[float, float]],
    lead_by_tipo_all: dict[str, tuple[float, float]],
    holidays: set[pd.Timestamp],
) -> dict[str, Any]:
    empty = {
        "month_dow": pd.DataFrame(columns=["month", "dow", "residuo_p80"]),
        "month": pd.DataFrame(columns=["month", "residuo_p80"]),
        "dow": pd.DataFrame(columns=["dow", "residuo_p80"]),
        "global": 0.0,
    }
    if service_assignments.empty:
        return empty

    hist_services = (
        service_assignments.groupby(["fecha_servicio", "tipo_servicio"], dropna=False)["service_id"]
        .nunique()
        .reset_index(name="eventos")
        .rename(columns={"fecha_servicio": "date"})
    )
    expected_hist_map = _allocate_expected_p50_by_prep_day(
        service_events=hist_services,
        ratio_tables=ratio_tables,
        urgency_tables=urgency_tables,
        urgencia_levels=urgencia_levels,
        lead_by_key=lead_by_key,
        lead_by_tipo_all=lead_by_tipo_all,
        holidays=holidays,
    )
    expected_hist = (
        pd.DataFrame({"date": list(expected_hist_map.keys()), "expected_p50": list(expected_hist_map.values())})
        if expected_hist_map
        else pd.DataFrame(columns=["date", "expected_p50"])
    )

    mov = join_movements.copy()
    tipo_mov_series = mov.get("tipo_movimiento", pd.Series(index=mov.index, dtype="object"))
    assigned_tipo_series = mov.get("assigned_tipo_servicio", pd.Series(index=mov.index, dtype="object"))
    mov["tipo_movimiento_norm"] = tipo_mov_series.astype(str).str.upper().str.strip()
    mov["assigned_tipo_servicio_norm"] = assigned_tipo_series.map(_normalize_service_type)
    mov["date"] = pd.to_datetime(mov["fecha_inicio_dia"], errors="coerce").dt.normalize()
    real = (
        mov[
            mov["is_assigned"].eq(1)
            & mov["tipo_movimiento_norm"].eq("PI")
            & mov["assigned_tipo_servicio_norm"].map(_is_delivery_type)
            & mov["date"].notna()
        ]
        .groupby("date", dropna=False)
        .size()
        .rename("real_picking")
        .reset_index()
    )

    dates = pd.concat(
        [
            expected_hist[["date"]] if not expected_hist.empty else pd.DataFrame(columns=["date"]),
            real[["date"]] if not real.empty else pd.DataFrame(columns=["date"]),
        ],
        ignore_index=True,
    ).drop_duplicates()
    if dates.empty:
        return empty

    merged = dates.merge(expected_hist, on="date", how="left").merge(real, on="date", how="left")
    merged["expected_p50"] = pd.to_numeric(merged["expected_p50"], errors="coerce").fillna(0.0)
    merged["real_picking"] = pd.to_numeric(merged["real_picking"], errors="coerce").fillna(0.0)
    merged["residuo"] = merged["real_picking"] - merged["expected_p50"]
    merged["month"] = merged["date"].dt.month.astype(int)
    merged["dow"] = merged["date"].dt.dayofweek.astype(int)

    month_dow = (
        merged.groupby(["month", "dow"], dropna=False)["residuo"]
        .quantile(0.8)
        .reset_index(name="residuo_p80")
    )
    by_month = (
        merged.groupby(["month"], dropna=False)["residuo"]
        .quantile(0.8)
        .reset_index(name="residuo_p80")
    )
    by_dow = (
        merged.groupby(["dow"], dropna=False)["residuo"]
        .quantile(0.8)
        .reset_index(name="residuo_p80")
    )
    global_resid = float(merged["residuo"].quantile(0.8)) if not merged.empty else 0.0
    if not np.isfinite(global_resid):
        global_resid = 0.0

    return {
        "month_dow": month_dow,
        "month": by_month,
        "dow": by_dow,
        "global": global_resid,
    }


def _lookup_residual_p80(residual_tables: dict[str, Any], month: int, dow: int) -> float:
    checks: list[tuple[str, dict[str, int]]] = [
        ("month_dow", {"month": month, "dow": dow}),
        ("month", {"month": month}),
        ("dow", {"dow": dow}),
    ]
    for level, filters in checks:
        table = residual_tables[level]
        if table.empty:
            continue
        mask = pd.Series(True, index=table.index)
        for key, val in filters.items():
            mask &= table[key].eq(val)
        if not mask.any():
            continue
        value = pd.to_numeric(table.loc[mask, "residuo_p80"], errors="coerce").dropna()
        if not value.empty:
            return float(value.iloc[0])

    global_resid = residual_tables.get("global", 0.0)
    if global_resid is None or pd.isna(global_resid):
        return 0.0
    return float(global_resid)


def _empty_expected_from_dates(dates: pd.Series) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "date": pd.to_datetime(dates, errors="coerce")
            .dropna()
            .dt.normalize()
            .drop_duplicates()
            .sort_values()
        }
    )
    out["axis"] = "workload_expected_from_service"
    out["tipo_servicio"] = "ALL"
    out["picking_movs_esperados_desde_servicio_p50"] = 0.0
    out["picking_movs_esperados_desde_servicio_p80"] = 0.0
    out["ratio_method"] = "ratio_mes_dow_entrega"
    out["workload_expected_movs_p50"] = out["picking_movs_esperados_desde_servicio_p50"]
    out["workload_expected_movs_p80"] = out["picking_movs_esperados_desde_servicio_p80"]
    out["movimientos_esperados_desde_servicio_p50"] = out["picking_movs_esperados_desde_servicio_p50"]
    out["movimientos_esperados_desde_servicio_p80"] = out["picking_movs_esperados_desde_servicio_p80"]
    return out


def transform_service_forecast_to_workload_expected(
    service_forecast_daily: pd.DataFrame,
    join_movements: pd.DataFrame,
    lead_time_summary: pd.DataFrame,
    holidays_df: pd.DataFrame,
    service_level_hist: pd.DataFrame,
) -> pd.DataFrame:
    sf = service_forecast_daily.copy()
    sf = sf[sf["axis"].eq("service")].copy()
    sf["date"] = pd.to_datetime(sf["date"], errors="coerce").dt.normalize()
    sf = sf[sf["date"].notna()].copy()
    if sf.empty:
        return _empty_expected_from_dates(pd.Series(dtype="datetime64[ns]"))

    sf["tipo_servicio"] = sf.get("tipo_servicio", "desconocida").map(_normalize_service_type)
    sf = sf[sf["tipo_servicio"].map(_is_delivery_type)].copy()
    if sf.empty:
        return _empty_expected_from_dates(service_forecast_daily.get("date", pd.Series(dtype="datetime64[ns]")))

    sf["eventos"] = pd.to_numeric(
        sf.get("conteo_servicios_p50", sf.get("conteo_servicios", 0.0)),
        errors="coerce",
    ).fillna(0.0)
    sf = sf.groupby(["date", "tipo_servicio"], dropna=False)["eventos"].sum().reset_index()

    service_assignments = _build_service_assignment_frame(join_movements, service_level_hist)
    if service_assignments.empty:
        return _empty_expected_from_dates(sf["date"])

    ratio_tables = _build_ratio_tables(service_assignments)
    urgency_tables = _build_urgency_shares(service_assignments)

    urgencia_levels = sorted(service_assignments["urgencia"].dropna().astype(str).unique().tolist())
    for urg in DEFAULT_URGENCIA_ORDER:
        if urg not in urgencia_levels:
            urgencia_levels.append(urg)
    if not urgencia_levels:
        urgencia_levels = DEFAULT_URGENCIA_ORDER.copy()

    holidays = set(pd.to_datetime(holidays_df["date"], errors="coerce").dropna().dt.normalize())
    lead_by_key, lead_by_tipo_all = _build_lead_lookup(lead_time_summary)

    p50_by_day = _allocate_expected_p50_by_prep_day(
        service_events=sf,
        ratio_tables=ratio_tables,
        urgency_tables=urgency_tables,
        urgencia_levels=urgencia_levels,
        lead_by_key=lead_by_key,
        lead_by_tipo_all=lead_by_tipo_all,
        holidays=holidays,
    )

    out = pd.DataFrame({"date": sorted(p50_by_day.keys())}) if p50_by_day else pd.DataFrame(columns=["date"])
    if out.empty:
        return _empty_expected_from_dates(sf["date"])

    out["axis"] = "workload_expected_from_service"
    out["tipo_servicio"] = "ALL"
    out["picking_movs_esperados_desde_servicio_p50"] = out["date"].map(p50_by_day).fillna(0.0).clip(lower=0.0)

    residual_tables = _build_residual_tables(
        service_assignments=service_assignments,
        join_movements=join_movements,
        ratio_tables=ratio_tables,
        urgency_tables=urgency_tables,
        urgencia_levels=urgencia_levels,
        lead_by_key=lead_by_key,
        lead_by_tipo_all=lead_by_tipo_all,
        holidays=holidays,
    )
    out["residual_p80"] = out.apply(
        lambda r: _lookup_residual_p80(
            residual_tables,
            month=int(pd.Timestamp(r["date"]).month),
            dow=int(pd.Timestamp(r["date"]).dayofweek),
        ),
        axis=1,
    )
    out["picking_movs_esperados_desde_servicio_p80"] = (
        out["picking_movs_esperados_desde_servicio_p50"] + out["residual_p80"]
    ).clip(lower=0.0)
    out["picking_movs_esperados_desde_servicio_p80"] = np.maximum(
        out["picking_movs_esperados_desde_servicio_p80"],
        out["picking_movs_esperados_desde_servicio_p50"],
    )
    out["ratio_method"] = "ratio_mes_dow_entrega_residual_p80"
    out = out.drop(columns=["residual_p80"], errors="ignore")

    # Compatibilidad hacia atras.
    out["workload_expected_movs_p50"] = out["picking_movs_esperados_desde_servicio_p50"]
    out["workload_expected_movs_p80"] = out["picking_movs_esperados_desde_servicio_p80"]
    out["movimientos_esperados_desde_servicio_p50"] = out["picking_movs_esperados_desde_servicio_p50"]
    out["movimientos_esperados_desde_servicio_p80"] = out["picking_movs_esperados_desde_servicio_p80"]
    return out.sort_values("date").reset_index(drop=True)
