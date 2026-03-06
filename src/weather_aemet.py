from __future__ import annotations

import json
import logging
import os
import time
import unicodedata
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
MAX_DAILY_RANGE_MONTHS = 6


def _env_int(name: str, default: int) -> int:
    try:
        val = int(os.getenv(name, str(default)).strip())
        return max(1, val)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        val = float(os.getenv(name, str(default)).strip())
        return max(0.1, val)
    except Exception:
        return default


HTTP_MAX_RETRIES = _env_int("AEMET_HTTP_MAX_RETRIES", 3)
HTTP_BACKOFF_SECONDS = _env_float("AEMET_HTTP_BACKOFF_SECONDS", 1.0)
MAX_PROVINCES_FETCH_PER_RUN = _env_int("AEMET_MAX_PROVINCES_FETCH_PER_RUN", 10)
MAX_FETCH_SECONDS = _env_int("AEMET_MAX_FETCH_SECONDS", 120)


@dataclass
class WeatherBuildResult:
    weighted_daily: pd.DataFrame
    province_daily: pd.DataFrame


def _ensure_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.normalize()


def _request_json_with_retry(url: str, *, params: dict | None = None, timeout: int = 30) -> object:
    if requests is None:
        raise RuntimeError("requests no disponible para llamadas AEMET")

    last_err: Exception | None = None
    for attempt in range(1, HTTP_MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            # AEMET rate limit and transient server-side errors.
            if r.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
            r.raise_for_status()
            return r.json()
        except Exception as exc:  # pragma: no cover
            last_err = exc
            if attempt >= HTTP_MAX_RETRIES:
                break
            sleep_s = HTTP_BACKOFF_SECONDS * attempt
            time.sleep(sleep_s)
    if last_err is None:
        raise RuntimeError("Error HTTP desconocido en AEMET")
    raise last_err


def _call_aemet_api(path: str, api_key: str) -> list[dict]:
    url = f"{AEMET_BASE}{path}"
    payload = _request_json_with_retry(url, params={"api_key": api_key}, timeout=30)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Respuesta AEMET inesperada: {str(payload)[:300]}")

    if "datos" not in payload:
        raise RuntimeError(f"Respuesta AEMET inesperada: {json.dumps(payload)[:500]}")

    data_url = payload["datos"]
    if not data_url:
        return []

    rows = _request_json_with_retry(data_url, timeout=60)
    if not isinstance(rows, list):
        raise RuntimeError(f"Respuesta AEMET datos no lista: {str(rows)[:300]}")
    return rows


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
    txt = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")
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
    today = pd.Timestamp.today().normalize()
    start = pd.Timestamp(start_date).normalize()
    end = min(pd.Timestamp(end_date).normalize(), today)
    if start > end:
        return pd.DataFrame(columns=["date", "tmed", "prec", "velmedia"])

    chunks: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    cur = start
    while cur <= end:
        next_start = cur + pd.DateOffset(months=MAX_DAILY_RANGE_MONTHS)
        chunk_end = min(end, next_start - pd.Timedelta(days=1))
        chunks.append((cur, chunk_end))
        cur = chunk_end + pd.Timedelta(days=1)

    chunk_rows: list[pd.DataFrame] = []
    for chunk_ini, chunk_fin in chunks:
        ini = chunk_ini.strftime("%Y-%m-%dT00:00:00UTC")
        fin = chunk_fin.strftime("%Y-%m-%dT23:59:59UTC")
        path = (
            "/valores/climatologicos/diarios/datos/"
            f"fechaini/{ini}/fechafin/{fin}/estacion/{station_id}"
        )
        try:
            rows = _call_aemet_api(path, api_key)
        except Exception as exc:
            msg = str(exc)
            if "429" in msg:
                raise
            # Algunas estaciones no tienen datos en tramos antiguos/concretos.
            if "No hay datos" in msg or "\"estado\": 404" in msg:
                continue
            LOGGER.warning("Fallo chunk meteo estacion=%s ini=%s fin=%s: %s", station_id, ini, fin, msg[:180])
            continue
        if rows:
            chunk_rows.append(pd.DataFrame(rows))
        time.sleep(0.2)

    if not chunk_rows:
        return pd.DataFrame(columns=["date", "tmed", "prec", "velmedia"])

    df = pd.concat(chunk_rows, ignore_index=True)
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df.get("fecha"), errors="coerce").dt.normalize()
    out["temp_media"] = df.get("tmed", pd.Series(dtype=object)).map(_clean_num)
    out["precip"] = df.get("prec", pd.Series(dtype=object)).map(_clean_num)
    out["viento_media"] = df.get("velmedia", pd.Series(dtype=object)).map(_clean_num)
    out = out.dropna(subset=["date"]).copy()
    out = out.drop_duplicates(subset=["date"], keep="last").sort_values("date")
    return out


def _province_has_full_range(
    weather_cache: pd.DataFrame,
    provincia_norm: str,
    min_day: pd.Timestamp,
    max_day: pd.Timestamp,
    min_coverage: float = 0.98,
) -> bool:
    if weather_cache.empty:
        return False
    sel = weather_cache[
        weather_cache["provincia_norm"].eq(provincia_norm)
        & weather_cache["date"].between(min_day, max_day)
    ]
    if sel.empty:
        return False
    expected_days = int((max_day - min_day).days + 1)
    if expected_days <= 0:
        return True
    coverage = sel["date"].nunique() / float(expected_days)
    return coverage >= min_coverage


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
    cache_station_inventory = outputs_dir / "aemet_station_inventory_cache.csv"
    cache_station_map = outputs_dir / "aemet_station_cache.csv"
    cache_weather = outputs_dir / "aemet_weather_cache.parquet"

    svc = service_level.copy()
    svc["fecha_servicio"] = _ensure_dt(svc["fecha_servicio"])
    svc = svc.dropna(subset=["fecha_servicio"]).copy()

    min_day = svc["fecha_servicio"].min()
    max_day = svc["fecha_servicio"].max()
    max_fetch_day = min(max_day, pd.Timestamp.today().normalize())

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
            inv = _fetch_station_inventory(api_key=api_key, cache_path=cache_station_inventory)
            station_map = _resolve_station_per_province(inv, provincia_station_map)
            station_map.to_csv(cache_station_map, index=False)

            # Prioriza MADRID para asegurar señal meteo base aunque haya rate-limit.
            province_weight = (
                svc.groupby("provincia_norm", dropna=False)["m3_out"].sum().to_dict()
                if "m3_out" in svc.columns
                else {}
            )
            station_map["prov_weight"] = station_map["provincia_norm"].map(lambda p: float(province_weight.get(p, 0.0)))
            station_map["priority"] = np.where(station_map["provincia_norm"].eq("MADRID"), 0, 1)
            station_map = station_map.sort_values(["priority", "prov_weight"], ascending=[True, False]).reset_index(drop=True)

            new_rows = []
            rate_limit_hits = 0
            fetched_provinces = 0
            fetch_start_ts = time.time()
            for _, row in station_map.iterrows():
                prov = row["provincia_norm"]
                station_id = row["station_id"]
                if prov not in province_list or not station_id:
                    continue
                if have_data and _province_has_full_range(cached, prov, min_day=min_day, max_day=max_fetch_day):
                    continue
                if fetched_provinces >= MAX_PROVINCES_FETCH_PER_RUN:
                    LOGGER.info("Limite de provincias meteo alcanzado en esta ejecucion (%d)", MAX_PROVINCES_FETCH_PER_RUN)
                    break
                if (time.time() - fetch_start_ts) >= MAX_FETCH_SECONDS:
                    LOGGER.info("Tiempo maximo de descarga meteo alcanzado (%.0fs)", MAX_FETCH_SECONDS)
                    break
                try:
                    daily = _fetch_station_daily(station_id, min_day, max_day, api_key)
                    if daily.empty:
                        continue
                    daily["provincia_norm"] = prov
                    daily["source"] = "aemet"
                    new_rows.append(daily)
                    fetched_provinces += 1
                    # Persistencia incremental: evita perder datos por timeout/interrupcion.
                    cached = pd.concat([cached, daily], ignore_index=True)
                    cached = cached.drop_duplicates(subset=["provincia_norm", "date"], keep="last")
                    _save_weather_cache(cached, cache_weather)
                    time.sleep(0.35)
                except Exception as exc:  # pragma: no cover
                    LOGGER.warning("Fallo descarga clima %s (%s): %s", prov, station_id, exc)
                    if "429" in str(exc):
                        rate_limit_hits += 1
                        if rate_limit_hits >= 4:
                            LOGGER.warning("Rate limit persistente AEMET; se usa cache/fallback para el resto de provincias")
                            break

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
