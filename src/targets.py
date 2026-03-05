from __future__ import annotations

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


def build_service_level(albaranes_clean: pd.DataFrame) -> pd.DataFrame:
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
    present_cols = [c for c in group_cols if c in albaranes_clean.columns]

    agg_dict = SERVICE_NUMERIC_AGGS.copy()
    agg_dict.update({"peso_facturable_total": "sum"})

    service_level = albaranes_clean.groupby(present_cols, dropna=False).agg(agg_dict).reset_index()
    return service_level


def _mix_ratio_by_date(service_level: pd.DataFrame) -> pd.DataFrame:
    cnt = (
        service_level.groupby(["fecha_servicio", "tipo_servicio_final"], dropna=False)["service_id"]
        .nunique()
        .reset_index(name="n")
    )
    piv = cnt.pivot(index="fecha_servicio", columns="tipo_servicio_final", values="n").fillna(0)

    entrega = piv["entrega"] if "entrega" in piv.columns else pd.Series(0, index=piv.index)
    recogida = piv["recogida"] if "recogida" in piv.columns else pd.Series(0, index=piv.index)
    denom = entrega + recogida
    ratio = (entrega / denom).where(denom > 0)

    return ratio.rename("mix_entrega_vs_recogida").reset_index()


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
    pivw = cntw.pivot(index="week_start", columns="tipo_servicio", values="n").fillna(0)
    ent = pivw["entrega"] if "entrega" in pivw.columns else pd.Series(0, index=pivw.index)
    rec = pivw["recogida"] if "recogida" in pivw.columns else pd.Series(0, index=pivw.index)
    week_mix = (ent / (ent + rec)).where((ent + rec) > 0).rename("mix_entrega_vs_recogida").reset_index().rename(columns={"week_start": "date"})

    weekly = weekly.merge(week_mix, on="date", how="left")
    weekly["axis"] = "service"

    return daily, weekly


def build_workload_targets(movements_joined: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    mov = movements_joined.copy()
    mov["date"] = pd.to_datetime(mov["fecha_inicio_dia"], errors="coerce").dt.normalize()

    base = (
        mov.groupby("date", dropna=False)
        .agg(
            conteo_movimientos=("fecha_inicio", "count"),
            unidades_movidas=("cantidad", "sum"),
            skus_distintos=("articulo", "nunique"),
            workload_atribuible_movs=("workload_atribuible", "sum"),
            workload_no_atribuible_movs=("workload_no_atribuible", "sum"),
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


def _is_non_working_day(day: pd.Timestamp, holidays: set[pd.Timestamp]) -> bool:
    return day.dayofweek >= 5 or day.normalize() in holidays


def _shift_to_business_day(day: pd.Timestamp, holidays: set[pd.Timestamp], direction: str) -> pd.Timestamp:
    d = day
    step = -1 if direction == "previous" else 1
    while _is_non_working_day(d, holidays):
        d = d + pd.Timedelta(days=step)
    return d


def transform_service_forecast_to_workload_expected(
    service_forecast_daily: pd.DataFrame,
    join_movements: pd.DataFrame,
    lead_time_summary: pd.DataFrame,
    holidays_df: pd.DataFrame,
) -> pd.DataFrame:
    sf = service_forecast_daily.copy()
    sf = sf[sf["axis"].eq("service")].copy()
    sf["date"] = pd.to_datetime(sf["date"], errors="coerce").dt.normalize()

    assigned = join_movements[join_movements["workload_atribuible"].eq(1)].copy()
    if assigned.empty:
        sf["workload_expected_movs_p50"] = 0.0
        sf["workload_expected_movs_p80"] = 0.0
        sf["workload_plan_date"] = sf["date"]
        sf["axis"] = "workload_expected_from_service"
        return sf

    assigned["fecha_inicio_dia"] = pd.to_datetime(assigned["fecha_inicio_dia"], errors="coerce").dt.normalize()
    ratio = (
        assigned.groupby("assigned_tipo_servicio", dropna=False)["fecha_inicio"]
        .count()
        .rename("movs")
        .reset_index()
        .merge(
            assigned[["assigned_service_id", "assigned_tipo_servicio"]].drop_duplicates().groupby("assigned_tipo_servicio").size().rename("services").reset_index(),
            on="assigned_tipo_servicio",
            how="left",
        )
    )
    ratio["movs_per_service"] = ratio["movs"] / ratio["services"].replace({0: np.nan})

    rmap = ratio.set_index("assigned_tipo_servicio")["movs_per_service"].to_dict()
    sf["movs_per_service_ratio"] = sf["tipo_servicio"].map(rmap).fillna(np.nanmean(list(rmap.values())) if rmap else 1.0)

    sf["workload_expected_movs_p50"] = sf.get("conteo_servicios_p50", sf.get("conteo_servicios", 0.0)) * sf["movs_per_service_ratio"]
    sf["workload_expected_movs_p80"] = sf.get("conteo_servicios_p80", sf.get("conteo_servicios", 0.0)) * sf["movs_per_service_ratio"]

    holidays = set(pd.to_datetime(holidays_df["date"], errors="coerce").dropna().dt.normalize())

    def _baseline_offset(tipo: str) -> int:
        if tipo == "entrega":
            return -3
        if tipo == "recogida":
            return 1
        return 0

    sf["workload_plan_date"] = sf.apply(lambda r: r["date"] + pd.Timedelta(days=_baseline_offset(r["tipo_servicio"])), axis=1)

    # Ajuste hábil: preparación (entrega) al hábil anterior; post (recogida) al siguiente.
    sf["workload_plan_date"] = sf.apply(
        lambda r: _shift_to_business_day(
            r["workload_plan_date"],
            holidays,
            direction="previous" if r["tipo_servicio"] == "entrega" else "next",
        ),
        axis=1,
    )

    lt = lead_time_summary.copy()
    lt = lt[lt["urgencia_norm"].eq("ALL")].copy()
    lt = lt[["tipo_servicio", "lead_time_pre_median", "lead_time_pre_p80", "lead_time_post_median", "lead_time_post_p80"]]

    sf = sf.merge(lt, on="tipo_servicio", how="left")
    sf["axis"] = "workload_expected_from_service"

    cols = [
        "workload_plan_date",
        "axis",
        "tipo_servicio",
        "workload_expected_movs_p50",
        "workload_expected_movs_p80",
        "lead_time_pre_median",
        "lead_time_pre_p80",
        "lead_time_post_median",
        "lead_time_post_p80",
    ]
    out = sf[cols].rename(columns={"workload_plan_date": "date"})

    # Consolidado diario total esperado.
    out = (
        out.groupby(["date", "axis", "tipo_servicio"], dropna=False)
        .agg(
            workload_expected_movs_p50=("workload_expected_movs_p50", "sum"),
            workload_expected_movs_p80=("workload_expected_movs_p80", "sum"),
            lead_time_pre_median=("lead_time_pre_median", "median"),
            lead_time_pre_p80=("lead_time_pre_p80", "median"),
            lead_time_post_median=("lead_time_post_median", "median"),
            lead_time_post_p80=("lead_time_post_p80", "median"),
        )
        .reset_index()
    )

    return out
