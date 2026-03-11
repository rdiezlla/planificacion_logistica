# Streamlit dashboard interno

Aplicacion Streamlit para visualizacion del forecast logistico y del basket analysis de picking.

## Instalacion

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements_streamlit.txt
```

Si ya usas el entorno del proyecto:

```powershell
.venv\Scripts\activate
pip install -r requirements_streamlit.txt
```

## Lanzamiento

```powershell
streamlit run streamlit_app/app.py
```

## Donde colocar los outputs

La app lee por defecto desde:

- `outputs/`
- `outputs_basket/`

Puedes cambiar ambas rutas desde el modulo `Settings`.

## Archivos usados por modulo

### Resumen

- `outputs/forecast_daily_business.csv`
- `outputs/forecast_weekly_business.csv`

### Transporte

- `outputs/forecast_daily_business.csv`
- `outputs/forecast_weekly_business.csv`

### Picking

- `outputs/forecast_daily_business.csv`
- `outputs/forecast_weekly_business.csv`
- `outputs/lead_time_summary.csv`

### Calidad del modelo

- `outputs/backtest_metrics.csv`
- `outputs/model_registry.csv`
- `outputs/join_kpis.csv`
- `outputs/lead_time_summary.csv`

### Optimizacion picking

- `outputs_basket/transactions_summary_oper.csv`
- `outputs_basket/transactions_summary_order.csv`
- `outputs_basket/sku_frequency_oper.csv`
- `outputs_basket/sku_frequency_order.csv`
- `outputs_basket/top_pairs_oper.csv`
- `outputs_basket/top_pairs_order.csv`
- `outputs_basket/rules_oper.csv`
- `outputs_basket/rules_order.csv`
- `outputs_basket/sku_clusters_oper.csv`
- `outputs_basket/sku_clusters_order.csv`
- `outputs_basket/order_owner_penalty.csv`
- `outputs_basket/sku_neighbors.csv` si existe
- `outputs_basket/plots/*` si existe

## Cambio de carpeta base

Desde `Settings` puedes editar:

- ruta de `outputs`
- ruta de `outputs_basket`
- escenario por defecto
- rango por defecto
- modo oscuro

Tambien puedes usar el boton `Recargar datos` para limpiar cache y releer disco.

## Notas de entorno corporativo Windows

- No necesita `npm`, `node` ni backend adicional.
- Usa `Pathlib` y lectura local de CSV.
- Si falta un archivo, la app no se rompe: muestra aviso y sigue cargando los modulos disponibles.
- `rules_oper.csv` puede ser pesado; la app lo lee en chunks filtrados para evitar cargas completas cuando no haga falta.

## Arquitectura

```text
streamlit_app/
  app.py
  pages/
  components/
  utils/
  .streamlit/config.toml
```

La capa reusable esta separada para poder anadir nuevos modulos sin rehacer la aplicacion.
