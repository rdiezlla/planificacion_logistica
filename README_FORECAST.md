# README_FORECAST

## Objetivo

El forecast engine genera las salidas de negocio que consumen los frontends.

- Fuente unica de verdad: `main.py` + `src/`
- No hay pipeline duplicado para web ni para Streamlit.

## Tres capas analiticas del forecast

El motor mantiene un solo pipeline, pero ahora deja mas explicitas tres capas de decision:

### 1) Forecast transporte

Pregunta de negocio:

- Que volumen de servicios y facturacion OUT / IN voy a tener por `fecha_servicio`.

Outputs principales:

- `outputs/forecast_daily_business.csv`
- `outputs/forecast_weekly_business.csv`
- `outputs/transport_forecast_daily.csv`
- `outputs/transport_forecast_weekly.csv`

Uso:

- planificacion comercial / transporte
- capacidad semanal OUT e IN
- seguimiento de m3, palets, cajas y peso facturable por servicio

### 2) Forecast workload operativo

Pregunta de negocio:

- Que carga operativa cae realmente por fecha de ejecucion en almacen.

Outputs principales:

- `outputs/workload_expected_daily.csv`
- `outputs/workload_expected_weekly.csv`

Uso:

- picking esperado de entregas
- inbound esperado de recogidas
- conversion de servicios a carga operativa real

Reglas actuales importantes:

- Picking esperado solo existe para entregas.
- Recogidas no generan picking.
- Inbound esperado se reparte post-servicio y ajustado a laborables Madrid.
- En inbound:
  - `CR` = recepcion / alta de paleta
  - `EP` = ubicacion

### 3) Forecast staffing

Pregunta de negocio:

- Cuantas horas / FTE necesito por proceso y semana o dia.

Outputs principales:

- `outputs/staffing_daily_plan.csv`
- `outputs/staffing_weekly_plan.csv`
- `data/labor_standards.csv`

Uso:

- planificacion de personal
- simulacion de sensibilidad cambiando productividades estandar
- conversion directa de workload a horas y FTE

## Entradas requeridas

En la raiz del repo:

- `Informacion_albaranaes.xlsx`
- `movimientos.xlsx`

En `data/`:

- `data/holidays_madrid.csv`
- `data/provincia_station_map.csv`

## Estructura del motor

- `main.py`: pipeline principal de forecast (daily/weekly).
- `src/`: limpieza, features, entrenamiento, backtest, prediccion y reporting.
- `outputs/`: salidas principales de forecast.
- `basket_main.py` + `src_basket/`: salidas de basket en `outputs_basket/`.
- `abc_main.py` + `src_abc/`: salidas ABC-XYZ en `outputs_abc/`.

## Comandos de ejecucion (pipeline principal)

### Mac/Linux

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py --horizon_days 60 --freq both --use_weather false
```

### Windows PowerShell

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python main.py --horizon_days 60 --freq both --use_weather false
```

## Regenerar datos antes de abrir Web o Streamlit

Siempre que quieras visualizar forecast actualizado:

1. Ejecuta `main.py`.
2. Si usas analitica basket, ejecuta `basket_main.py`.
3. Si usas analitica ABC, ejecuta `abc_main.py`.
4. Abre el frontend (web o Streamlit) sin recalcular modelos en el frontend.

## Salidas principales y para que sirve cada una

### `outputs/forecast_daily_business.csv`

Forecast diario de negocio para transporte (OUT/IN), picking esperado diario y capa inbound esperada.

### `outputs/forecast_weekly_business.csv`

Forecast semanal agregado de negocio (OUT/IN), picking esperado semanal e inbound esperado semanal.

### `outputs/workload_expected_daily.csv`

Forecast diario derivado desde servicios hacia workload operativo.

Incluye al menos:

- `picking_movs_esperados_desde_servicio_*`
- `inbound_recepcion_cr_esperados_*`
- `inbound_ubicacion_ep_esperados_*`
- proxies inbound por `pales_in`, `cajas_in`, `m3_in`

### `outputs/workload_expected_weekly.csv`

Agregado semanal de la capa anterior para staffing y capacity planning.

### `outputs/staffing_daily_plan.csv`

Conversion diaria de drivers operativos a:

- horas requeridas
- FTE requeridos P50 / P80

Procesos iniciales:

- `picking_out`
- `inbound_recepcion`
- `inbound_ubicacion`
- `no_atribuible`

### `outputs/staffing_weekly_plan.csv`

Plan semanal de horas y FTE. Es la salida mas util para staffing / capacity review.

### `outputs/backtest_metrics.csv`

Metricas de calidad del modelo por eje, frecuencia y target.

Ademas de WAPE / sMAPE / MASE, ahora incluye:

- `empirical_coverage_p80`
- `pinball_loss_p50`
- `pinball_loss_p80`

Interpretacion:

- `P50`: escenario central.
- `P80`: escenario alto razonable.
- `empirical_coverage_p80`: porcentaje historico donde el real quedo por debajo o igual al P80.
  Si esta cerca de `0.80`, el cuantil esta bien calibrado.

### `outputs/model_registry.csv`

Registro de artefactos/modelos entrenados y su trazabilidad.

### `outputs/model_health_summary.csv`

Resumen sintetico para monitorizacion del motor:

- cutoff usado
- coverage P80
- WAPE
- modelo elegido
- flags de riesgo

### `outputs/service_type_audit.csv`

Auditoria de cobertura de tipologias:

- entrega
- recogida
- mixto
- desconocida

Sirve para detectar si hay demasiada ambiguedad en la clasificacion de servicios.

### `outputs/service_intensity_summary.csv`

Diagnostico de intensidad por evento, por tipo de servicio y mes:

- m3 por evento
- palets por evento
- cajas por evento
- peso facturable por evento

### `outputs/feature_policy.csv`

Politica de features excluidas del entrenamiento para evitar leakage / skew.

Actualmente documenta la exclusion de:

- `movtype_*`
- flags de calendario de trazabilidad (`had_raw_record`, `was_zero_filled`, `calendar_status`, etc.)

### `outputs_basket/*`

Salidas del modulo basket para optimizacion operativa de picking:

- resumen de transacciones
- pares/trios frecuentes
- reglas de asociacion
- clusters de SKU
- penalizacion multi-propietario
- plots

### `outputs_abc/*`

Salidas del modulo ABC-XYZ para priorizacion de SKU y layout:

- clases ABC/XYZ por periodo
- resumenes por periodo y owner
- cambios entre periodos
- recomendaciones de layout
- plots

## Comandos utiles de modulos complementarios

### Basket

```bash
python basket_main.py --input movimientos.xlsx --output_dir outputs_basket
```

### ABC

```bash
python abc_main.py --input movimientos.xlsx --output_dir outputs_abc
```

## Conexion con los frontends

- Web (`web/`): consume CSV ya generados en `outputs/`.
- Streamlit (`streamlit_app/`): consume CSV ya generados en `outputs/`, `outputs_basket/`, `outputs_abc/`.

Ningun frontend recalcula modelos.

## Configuracion de staffing

El archivo `data/labor_standards.csv` controla los supuestos operativos sin tocar codigo:

- `process`
- `driver`
- `driver_base`
- `productivity_rate`
- `allowance_pct`
- `setup_hours`
- `shift_hours`
- `utilization_effective`

Formula base:

```text
horas_requeridas = (driver_forecast / productivity_rate) * (1 + allowance_pct) + setup_hours
fte = horas_requeridas / (shift_hours * utilization_effective)
```

## Nota critica sobre 2025

`2025` se trata como periodo `blackout / sin observacion`, no como demanda cero.

Eso significa:

- ausencia de registros en 2025 no se interpreta como cero real
- el calendario densificado diferencia:
  - `observed`
  - `valid_zero`
  - `blackout`

## Recomendacion de uso por decision

- Transporte / facturacion:
  usar `forecast_weekly_business.csv`
- Workload operativo:
  usar `workload_expected_daily.csv` o `workload_expected_weekly.csv`
- Staffing:
  usar `staffing_weekly_plan.csv`
- Calidad / monitorizacion:
  usar `backtest_metrics.csv` + `model_health_summary.csv`
