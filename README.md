# Forecasting end-to-end de operativa logística (servicios + workload)

Pipeline completo en Python para forecasting con dos ejes temporales independientes:

1. `FECHA_SERVICIO` (albaranes): demanda/servicio/facturación.
2. `Fecha_inicio` (movimientos): workload real de almacén.

## Estructura

- `src/io.py`: carga y validación de inputs.
- `src/cleaning.py`: limpieza, normalización, imputación conservadora, urgencia.
- `src/service_id.py`: clave `service_id=(codigo_norm,fecha_servicio)`.
- `src/service_classification.py`: reglas deterministas de tipo de servicio.
- `src/join_assignment.py`: asignación movimientos->`service_id` por código + ventana temporal.
- `src/targets.py`: targets diarios/semanales + transformación A->B.
- `src/holidays.py`: features de festivos Madrid.
- `src/easter.py`: cálculo de Pascua/Semana Santa.
- `src/calendar_features.py`: features de calendario.
- `src/geo_normalization.py`: normalización de provincia destino.
- `src/weather_aemet.py`: integración AEMET OpenData con caché y fallback.
- `src/feature_engineering.py`: ensamblado de features sin fuga.
- `src/backtest.py`: walk-forward rolling origin + métricas.
- `src/train.py`: entrenamiento modelos y persistencia.
- `src/predict.py`: predicción con artefactos.
- `src/report.py`: gráficos de diagnóstico.
- `main.py`: orquestación CLI end-to-end.

## Inputs esperados (root del repo)

- `./Informacion_albaranaes.xlsx`
- `./movimientos.xlsx`
- `./data/holidays_madrid.csv`
- `./data/provincia_station_map.csv`

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuración AEMET

La clave se lee **siempre** desde variable de entorno:

```bash
export AEMET_API_KEY="TU_API_KEY"
```

Si no está definida o falla la API, el pipeline sigue con fallback (`MADRID`/climatología/cache) y flags.

## Ejecución

Ejemplo solicitado:

```bash
python main.py --horizon_days 60 --freq both --use_weather true
```

Parámetros:

- `--horizon_days` (default `60`)
- `--freq` (`daily|weekly|both`, default `both`)
- `--use_weather` (`true|false`, default `true`)
- `--assignment_window_days` (default `30`)
- `--exclude_years` (default `2025`, lista separada por coma)
- `--cutoff_date` (opcional; formato `YYYY-MM-DD`, si no usa hoy)

## Salidas

- `outputs/forecast_daily.csv`
- `outputs/forecast_weekly.csv`
- `outputs/backtest_metrics.csv`
- `outputs/join_kpis.csv`
- `outputs/lead_time_summary.csv`
- `outputs/model_registry.csv`
- `outputs/diagnostics/*.png`
- `outputs/aemet_station_cache.csv` (cache)
- `outputs/aemet_weather_cache.parquet` (cache)

## Validación y métricas

Backtesting walk-forward (sin barajar) con:

- WAPE (principal)
- sMAPE
- MASE
- métricas en picos (top 5% por target)

Incluye baselines `naive_t7`, `ma7`, `ma28` y modelo principal (`LightGBMRegressor` con fallback a `HistGradientBoostingRegressor`).

## Notas de negocio implementadas

- No mezcla ejes temporales (`FECHA_SERVICIO` vs `Fecha_inicio`).
- Entrenamiento de servicios solo con histórico (`fecha_servicio <= cutoff`).
- Excluye 2025 por defecto de entrenamiento/scoring (configurable).
- `Pedido externo` nulo tratado como workload no atribuible.
- `service_id` compuesto por `(codigo_norm, fecha_servicio)`.
- Join por código normalizado (quita sufijos `-01`, `-02`...) y fecha más cercana dentro de `±assignment_window_days`.
- Clasificación determinista de tipo servicio + `tipo_servicio_regla`.
- Cálculo de peso facturable y split en mixtos con `split_rule`.
- Imputación conservadora + flags `has_*` y `*_imputed`.
