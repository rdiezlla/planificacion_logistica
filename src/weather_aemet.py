from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

LOGGER = logging.getLogger(__name__)

AEMET_BASE = "https://opendata.aemet.es/opendata/api"


@dataclass
class WeatherBuildResult:
    weighted_daily: pd.DataFrame
    province_daily: pd.DataFrame


def _ensure_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.normalize()


def _call_aemet_api(path: str, api_key: str) -> list[dict]:
    if requests is None:
        raise RuntimeError("requests no disponible para llamadas AEMET")

    url = f"{AEMET_BASE}{path}"
    r = requests.get(url, params={"api_key": api_key}, timeout=30)
    r.raise_for_status()
    payload = r.json()

    if "datos" not in payload:
        raise RuntimeError(f"Respuesta AEMET inesperada: {json.dumps(payload)[:500]}")

    data_url = payload["datos"]
    if not data_url:
        return []

    r2 = requests.get(data_url, timeout=60)
    r2.raise_for_status()
    return r2.json()


def _clean_num(value: object) -> float:
    if value is None:
        return np.nan
    s = str(value).strip().replace(",", ".")
    if s in {"", "Ip", "NaN", "nan"}:
        return np.nan
    try:
        return float(s)
    except ValueError:
        return np.nan


def _fetch_station_inventory(api_key: str, cache_path: Path) -> pd.DataFrame:
    if cache_path.exists():
        try:
            df = pd.read_csv(cache_path)
            if {"indicativo", "provincia", "nombre"}.issubset(df.columns):
                return df
        except Exception as exc:  # pragma: no cover
            LOGGER.warning("No se pudo leer cache de estaciones (%s): %s", cache_path, exc)

    rows = _call_aemet_api("/valores/climatologicos/inventarioestaciones/todasestaciones", api_key)
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("AEMET devolvio inventario vacio")

    keep = [c for c in ["indicativo", "provincia", "nombre", "latitud", "longitud", "altitud"] if c in df.columns]
    out = df[keep].copy()
    out.to_csv(cache_path, index=False)
    return out


def _norm(s: object) -> str:
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return ""
    txt = str(s).strip().upper()
    txt = (
        txt.replace("Á", "A")
        .replace("É", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ú", "U")
        .replace("Ü", "U")
    )
    return " ".join(txt.split())


def _resolve_station_per_province(
    station_inventory: pd.DataFrame,
    provincia_station_map: pd.DataFrame,
) -> pd.DataFrame:
    inv = station_inventory.copy()
    inv["provincia_key"] = inv["provincia"].map(_norm)
    inv["nombre_key"] = inv["nombre"].map(_norm)

    mapping = provincia_station_map.copy()
    mapping["capital_key"] = mapping["capital"].map(_norm)
    mapping["provincia_key"] = mapping["provincia_norm"].map(_norm)

    rows = []
    for _, r in mapping.iterrows():
        prov_key = r["provincia_key"]
        cap_key = r["capital_key"]

        cand = inv[inv["provincia_key"].eq(prov_key)]
        if cand.empty:
            cand = inv[inv["provincia_key"].str.contains(prov_key.split(" ")[0], na=False)]

        chosen = None
        if not cand.empty:
            exact_cap = cand[cand["nombre_key"].str.contains(cap_key, na=False)]
            if not exact_cap.empty:
                chosen = exact_cap.iloc[0]
            else:
                chosen = cand.iloc[0]

        rows.append(
            {
                "provincia_norm": r["provincia_norm"],
                "capital": r.get("capital"),
                "station_id": chosen["indicativo"] if chosen is not None else None,
                "station_name": chosen["nombre"] if chosen is not None else None,
                "station_provincia": chosen["provincia"] if chosen is not None else None,
            }
        )

    return pd.DataFrame(rows)


def _fetch_station_daily(
    station_id: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    api_key: str,
) -> pd.DataFrame:
    ini = start_date.strftime("%Y-%m-%dT00:00:00UTC")
    fin = end_date.strftime("%Y-%m-%dT23:59:59UTC")
    path = f"/valores/climatologicos/diarios/datos/fechaini/{ini}/fechafin/{fin}/estacion/{station_id}"

    rows = _call_aemet_api(path, api_key)
    if not rows:
        return pd.DataFrame(columns=["date", "tmed", "prec", "velmedia"])

    df = pd.DataFrame(rows)
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df.get("fecha"), errors="coerce").dt.normalize()
    out["temp_media"] = df.get("tmed", pd.Series(dtype=object)).map(_clean_num)
    out["precip"] = df.get("prec", pd.Series(dtype=object)).map(_clean_num)
    out["viento_media"] = df.get("velmedia", pd.Series(dtype=object)).map(_clean_num)
    out = out.dropna(subset=["date"]).copy()
    return out


def _load_weather_cache(cache_weather: Path) -> pd.DataFrame:
    if cache_weather.exists():
        try:
            return pd.read_parquet(cache_weather)
        except Exception as exc:  # pragma: no cover
            LOGGER.warning("No se pudo leer cache meteo parquet (%s): %s", cache_weather, exc)
    return pd.DataFrame(columns=["provincia_norm", "date", "temp_media", "precip", "viento_media", "source"])


def _save_weather_cache(df: pd.DataFrame, cache_weather: Path) -> None:
    cache_weather.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_weather, index=False)


def build_weighted_weather(
    service_level: pd.DataFrame,
    provincia_station_map: pd.DataFrame,
    outputs_dir: Path,
    use_weather: bool = True,
) -> WeatherBuildResult:
    cache_station = outputs_dir / "aemet_station_cache.csv"
    cache_weather = outputs_dir / "aemet_weather_cache.parquet"

    svc = service_level.copy()
    svc["fecha_servicio"] = _ensure_dt(svc["fecha_servicio"])
    svc = svc.dropna(subset=["fecha_servicio"]).copy()

    min_day = svc["fecha_servicio"].min()
    max_day = svc["fecha_servicio"].max()

    if not use_weather:
        LOGGER.info("Clima desactivado por flag --use_weather=false")
        empty = pd.DataFrame(
            {
                "date": pd.date_range(min_day, max_day, freq="D"),
                "temp_media_weighted": np.nan,
                "precip_weighted": np.nan,
                "viento_weighted": np.nan,
                "weather_source": "disabled",
                "weather_fallback": 1,
            }
        )
        return WeatherBuildResult(weighted_daily=empty, province_daily=pd.DataFrame())

    api_key = os.getenv("AEMET_API_KEY", "").strip()
    cached = _load_weather_cache(cache_weather)
    cached["date"] = _ensure_dt(cached.get("date", pd.Series(dtype=object)))

    province_list = sorted(
        p for p in svc.get("provincia_norm", pd.Series(dtype=str)).dropna().unique() if p != "DESCONOCIDA"
    )

    have_data = not cached.empty
    need_fetch = bool(api_key) and requests is not None

    if need_fetch:
        try:
            inv = _fetch_station_inventory(api_key=api_key, cache_path=cache_station)
            station_map = _resolve_station_per_province(inv, provincia_station_map)
            station_map.to_csv(cache_station, index=False)

            existing_pairs = set()
            if have_data:
                existing_pairs = set(
                    cached.loc[cached["date"].between(min_day, max_day), "provincia_norm"].dropna().unique()
                )

            new_rows = []
            for _, row in station_map.iterrows():
                prov = row["provincia_norm"]
                station_id = row["station_id"]
                if prov not in province_list or not station_id:
                    continue
                if prov in existing_pairs:
                    continue
                try:
                    daily = _fetch_station_daily(station_id, min_day, max_day, api_key)
                    if daily.empty:
                        continue
                    daily["provincia_norm"] = prov
                    daily["source"] = "aemet"
                    new_rows.append(daily)
                    time.sleep(0.2)
                except Exception as exc:  # pragma: no cover
                    LOGGER.warning("Fallo descarga clima %s (%s): %s", prov, station_id, exc)

            if new_rows:
                add = pd.concat(new_rows, ignore_index=True)
                cached = pd.concat([cached, add], ignore_index=True)
                cached = cached.drop_duplicates(subset=["provincia_norm", "date"], keep="last")
                _save_weather_cache(cached, cache_weather)
        except Exception as exc:  # pragma: no cover
            LOGGER.warning("Fallo integracion AEMET, se usara fallback: %s", exc)

    weather = cached.copy()
    if weather.empty:
        base_dates = pd.date_range(min_day, max_day, freq="D")
        weighted = pd.DataFrame(
            {
                "date": base_dates,
                "temp_media_weighted": np.nan,
                "precip_weighted": np.nan,
                "viento_weighted": np.nan,
                "weather_source": "fallback_empty",
                "weather_fallback": 1,
            }
        )
        return WeatherBuildResult(weighted_daily=weighted, province_daily=weather)

    weather["date"] = _ensure_dt(weather["date"])
    weather = weather.dropna(subset=["date", "provincia_norm"]).copy()

    w = (
        svc.groupby(["fecha_servicio", "provincia_norm"], dropna=False)
        .agg(
            m3_out=("m3_out", "sum"),
            peso_facturable_out=("peso_facturable_out", "sum"),
            conteo_servicios=("service_id", "nunique"),
        )
        .reset_index()
        .rename(columns={"fecha_servicio": "date"})
    )

    weight = np.where(w["m3_out"] > 0, w["m3_out"], np.where(w["peso_facturable_out"] > 0, w["peso_facturable_out"], w["conteo_servicios"]))
    w["weight"] = weight

    merged = w.merge(weather, on=["date", "provincia_norm"], how="left")

    madrid = weather[weather["provincia_norm"].eq("MADRID")][["date", "temp_media", "precip", "viento_media"]].copy()
    madrid = madrid.rename(
        columns={
            "temp_media": "temp_madrid",
            "precip": "precip_madrid",
            "viento_media": "viento_madrid",
        }
    )
    merged = merged.merge(madrid, on="date", how="left")

    for c, m in [
        ("temp_media", "temp_madrid"),
        ("precip", "precip_madrid"),
        ("viento_media", "viento_madrid"),
    ]:
        merged[c] = merged[c].fillna(merged[m])

    def _weighted_avg(g: pd.DataFrame, col: str) -> float:
        vals = g[col]
        ws = g["weight"]
        ok = vals.notna() & ws.notna() & (ws > 0)
        if not ok.any():
            return np.nan
        return float(np.average(vals[ok], weights=ws[ok]))

    weighted = (
        merged.groupby("date", dropna=False)
        .apply(
            lambda g: pd.Series(
                {
                    "temp_media_weighted": _weighted_avg(g, "temp_media"),
                    "precip_weighted": _weighted_avg(g, "precip"),
                    "viento_weighted": _weighted_avg(g, "viento_media"),
                }
            )
        )
        .reset_index()
    )

    weighted["weather_source"] = np.where(weighted["temp_media_weighted"].notna(), "aemet_or_cache", "fallback")
    weighted["weather_fallback"] = weighted["temp_media_weighted"].isna().astype(int)

    weighted = weighted.sort_values("date")
    for c in ["temp_media_weighted", "precip_weighted", "viento_weighted"]:
        weighted[f"{c}_roll3"] = weighted[c].rolling(3, min_periods=1).mean()
        weighted[f"{c}_roll7"] = weighted[c].rolling(7, min_periods=1).mean()

    return WeatherBuildResult(weighted_daily=weighted, province_daily=weather)


def extend_weather_with_climatology(
    weather_daily: pd.DataFrame,
    target_dates: pd.Series,
) -> pd.DataFrame:
    td = pd.to_datetime(target_dates, errors="coerce").dt.normalize()
    base = pd.DataFrame({"date": td}).drop_duplicates().sort_values("date")

    if weather_daily.empty:
        base["temp_media_weighted"] = np.nan
        base["precip_weighted"] = np.nan
        base["viento_weighted"] = np.nan
        base["weather_source"] = "fallback_empty"
        base["weather_fallback"] = 1
        for c in ["temp_media_weighted", "precip_weighted", "viento_weighted"]:
            base[f"{c}_roll3"] = np.nan
            base[f"{c}_roll7"] = np.nan
        return base

    w = weather_daily.copy()
    w["date"] = pd.to_datetime(w["date"], errors="coerce").dt.normalize()
    w = w.dropna(subset=["date"]).copy()

    out = base.merge(w, on="date", how="left")

    if out[["temp_media_weighted", "precip_weighted", "viento_weighted"]].isna().any().any():
        clim = w.copy()
        clim["month"] = clim["date"].dt.month
        clim["day"] = clim["date"].dt.day
        clim_ref = (
            clim.groupby(["month", "day"], dropna=False)[
                ["temp_media_weighted", "precip_weighted", "viento_weighted"]
            ]
            .mean()
            .reset_index()
        )

        out["month"] = out["date"].dt.month
        out["day"] = out["date"].dt.day
        out = out.merge(clim_ref, on=["month", "day"], how="left", suffixes=("", "_clim"))
        for c in ["temp_media_weighted", "precip_weighted", "viento_weighted"]:
            out[c] = out[c].fillna(out[f"{c}_clim"])
            out = out.drop(columns=[f"{c}_clim"])
        out = out.drop(columns=["month", "day"])
        out["weather_source"] = out["weather_source"].fillna("climatology")

    out["weather_fallback"] = out["temp_media_weighted"].isna().astype(int)
    out = out.sort_values("date")
    for c in ["temp_media_weighted", "precip_weighted", "viento_weighted"]:
        out[f"{c}_roll3"] = out[c].rolling(3, min_periods=1).mean()
        out[f"{c}_roll7"] = out[c].rolling(7, min_periods=1).mean()

    return out
