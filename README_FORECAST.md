# README_FORECAST

## Objetivo

El forecast engine genera las salidas de negocio que consumen los frontends.

- Fuente unica de verdad: `main.py` + `src/`
- No hay pipeline duplicado para web ni para Streamlit.

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

Forecast diario de negocio para transporte (OUT/IN) y picking esperado diario.

### `outputs/forecast_weekly_business.csv`

Forecast semanal agregado de negocio (OUT/IN) y picking esperado semanal.

### `outputs/backtest_metrics.csv`

Metricas de calidad del modelo (WAPE, sMAPE, MASE, etc.) por eje, frecuencia y target.

### `outputs/model_registry.csv`

Registro de artefactos/modelos entrenados y su trazabilidad.

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
