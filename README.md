# Forecasting end-to-end de operativa logistica (servicios + workload)

Pipeline completo en Python para forecasting con dos ejes temporales independientes:

1. `fecha_servicio` (albaranes): demanda/servicio/facturacion.
2. `fecha_inicio` (movimientos): workload real de almacen.

Metrica principal de almacen para planificacion operativa: `picking_movs` (lineas de movimiento `PI`), no unidades.

## Estructura

- `src/io.py`: carga de inputs + estandarizacion robusta de headers.
- `src/cleaning.py`: limpieza, parsing de fechas con flags, imputacion conservadora, codigos y urgencia.
- `src/service_id.py`: extraccion/normalizacion de codigos y clave `service_id=(codigo_norm, fecha_servicio)`.
- `src/service_classification.py`: reglas deterministas de tipo de servicio.
- `src/join_assignment.py`: consolidacion y asignacion movimientos->`service_id`, KPIs y debug del join.
- `src/targets.py`: targets diarios/semanales + transformacion A->B.
- `src/holidays.py`: features de festivos Madrid.
- `src/easter.py`: calculo de Pascua/Semana Santa.
- `src/calendar_features.py`: features de calendario.
- `src/geo_normalization.py`: normalizacion de provincia destino.
- `src/weather_aemet.py`: integracion AEMET OpenData con cache y fallback.
- `src/feature_engineering.py`: ensamblado de features sin fuga.
- `src/backtest.py`: walk-forward rolling origin + metricas.
- `src/train.py`: entrenamiento de modelos y persistencia.
- `src/predict.py`: prediccion con artefactos.
- `src/report.py`: graficos de diagnostico.
- `main.py`: orquestacion CLI end-to-end.

## Inputs esperados (root del repo)

- `./Informacion_albaranaes.xlsx`
- `./movimientos.xlsx`
- `./data/holidays_madrid.csv`
- `./data/provincia_station_map.csv`

## Instalacion

### Linux/macOS

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Windows PowerShell

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Configuracion AEMET

La clave se lee desde variable de entorno:

```bash
export AEMET_API_KEY="TU_API_KEY"
```

En PowerShell:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
$env:AEMET_API_KEY="TU_API_KEY"
```

Si no esta definida o falla la API, el pipeline sigue con fallback (`MADRID`/climatologia/cache) y flags.

Opcional (control de descarga en entornos corporativos/rate-limit):

```powershell
$env:AEMET_MAX_PROVINCES_FETCH_PER_RUN="10"
$env:AEMET_MAX_FETCH_SECONDS="120"
$env:AEMET_HTTP_MAX_RETRIES="3"
$env:AEMET_HTTP_BACKOFF_SECONDS="1.0"
```

## Ejecucion

Ejemplo principal:

```bash
python main.py --horizon_days 60 --freq both --use_weather true
```

Parametros:

- `--horizon_days` (default `60`)
- `--freq` (`daily|weekly|both`, default `both`)
- `--use_weather` (`true|false`, default `true`)
- `--assignment_window_days` (default `30`)
- `--debug_join` (`true|false`, default `false`)
- `--exclude_years` (default `2025`, lista separada por coma)
- `--cutoff_date` (opcional, formato `YYYY-MM-DD`; si no usa hoy)

## Join movimientos -> albaranes

- Estandariza columnas (espacios/tildes/aliases).
- Extrae codigo de `albaranes.descripcion` y `movimientos.pedido_externo/pedido`.
- `codigo_norm`: `upper(strip)` + elimina sufijos finales `-01`, `-02`, `/01`.
- `service_id` se mantiene como `(codigo_norm_alb, fecha_servicio)`.
- Antes del join, albaranes se consolidan por `(codigo_norm_alb, fecha_servicio)`.
- Asignacion por codigo:
  - si hay una sola fecha de servicio para el codigo, asigna.
  - si hay varias, toma la fecha mas cercana a `fecha_inicio` dentro de `+-assignment_window_days`.
- Si el codigo contiene anio embebido (4 digitos tras prefijo), se filtra por anio de `fecha_servicio`.
- Si `mov_asignados < 5%`, activa matching secundario por regex en columnas texto (sin fuzzy).

## Modo debug rapido del join

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\activate
python main.py --debug_join true
```

Genera:

- `outputs/join_debug_summary.csv`
- `outputs/join_debug_samples.csv`
- `outputs/join_debug_intersection_top.csv`

## Ejecucion completa en Windows

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\activate
python main.py --horizon_days 60 --freq both --use_weather false --assignment_window_days 120
```

## Logica de workload esperado (picking solo entregas)

- `picking_movs_esperados_desde_servicio_*` se calcula **solo** con servicios de entrega (`tipo_servicio` que empieza por `entrega`).
- Servicios de recogida (`recogida`) **no** participan en el expected de picking.
- Ratio base historico (robusto):
  - `ratio_p50 = mediana(picking_movs_atribuibles_entrega / eventos_entrega)` segmentado por `(mes, day_of_week)` con fallback global.
- Base diaria:
  - `expected_p50 = ratio_p50 * eventos_entrega_p50`.
- Distribucion temporal de preparacion (solo entregas):
  - usa `lead_time_summary` por urgencia/tipo si existe y es util.
  - fallback determinista: `NO=3` dias, `SI=1` dia, `MUY_URGENTE=0` dias.
- Ajuste de calendario Madrid:
  - laborable = lunes-viernes y no festivo (`data/holidays_madrid.csv`, incluyendo 2026).
  - si cae en no laborable, mueve al dia habil anterior.
- P80 calibrado por residuos (no inflado):
  - en historico se calcula `residuo = real_picking_entrega - expected_p50`.
  - se toma `residuo_p80` por `(mes, day_of_week)` (fallback global).
  - `expected_p80 = max(expected_p50, expected_p50 + residuo_p80_segmentado)`.

## Salidas

- `outputs/forecast_daily.csv`
- `outputs/forecast_weekly.csv`
- `outputs/forecast_daily_business.csv`
- `outputs/forecast_weekly_business.csv`
- `outputs/backtest_metrics.csv`
- `outputs/join_kpis.csv`
- `outputs/lead_time_summary.csv`
- `outputs/model_registry.csv`
- `outputs/diagnostics/*.png`
- `outputs/aemet_station_cache.csv`
- `outputs/aemet_weather_cache.parquet`

## Interpretacion de forecast_daily_business.csv

- `fecha`: dia calendario.
- Transporte ENTREGA (fecha_servicio): `eventos_entrega_p50/p80`, `m3_out_p50/p80`, `pales_out_p50/p80`, `cajas_out_p50/p80`, `peso_facturable_out_p50/p80`.
- Transporte RECOGIDA (fecha_servicio): `eventos_recogida_p50/p80`, `m3_in_p50/p80`, `pales_in_p50/p80`, `cajas_in_p50/p80`, `peso_facturable_in_p50/p80`.
- Almacen (fecha de preparacion): `picking_movs_esperados_p50/p80` (solo derivado de entregas).

## Interpretacion de forecast_weekly_business.csv

Columnas de calendario:
- `week_iso`, `year`, `week_start_date`, `week_end_date`.

Transporte semanal (por FECHA_SERVICIO):
- ENTREGA/OUT: `eventos_entrega_semana_p50/p80`, `m3_out_semana_p50/p80`, `pales_out_semana_p50/p80`, `cajas_out_semana_p50/p80`, `peso_facturable_out_semana_p50/p80`.
- RECOGIDA/IN: `eventos_recogida_semana_p50/p80`, `m3_in_semana_p50/p80`, `pales_in_semana_p50/p80`, `cajas_in_semana_p50/p80`, `peso_facturable_in_semana_p50/p80`.

Almacen semanal:
- `picking_movs_esperados_semana_p50/p80`: picking esperado por fecha de preparacion, derivado solo de entregas.
- `picking_movs_reales_semana`: picking real historico (comparacion).
- `picking_movs_no_atribuibles_semana` (y `_p50/_p80`): picking no atribuible a servicio.

Uso recomendado:
- Plan base: `P50` (caso central).
- Capacidad preventiva: `P80` (alto realista calibrado con residuos).
- Si `picking_movs_esperados_semana_p80` supera capacidad nominal, programar refuerzo de turnos.

## Validacion y metricas

Backtesting walk-forward (sin barajar) con:

- WAPE (principal)
- sMAPE
- MASE
- metricas en picos (top 5% por target)

Incluye baselines `naive_t7`, `ma7`, `ma28` y modelo principal (`LightGBMRegressor` con fallback a `HistGradientBoostingRegressor`).
Adicionalmente se reporta backtest especifico del expected de picking (entrega):
- WAPE diario/semanal contra `picking_movs_atribuibles_entrega`.
- cobertura empirica `%dias real <= p80`.
