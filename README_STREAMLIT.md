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
- `outputs_abc/`

Puedes cambiar esas rutas desde el modulo `Settings`.

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

### ABC Picking

- `outputs_abc/abc_picking_annual.csv`
- `outputs_abc/abc_picking_quarterly.csv`
- `outputs_abc/abc_picking_ytd.csv`
- `outputs_abc/abc_summary_by_period.csv`
- `outputs_abc/abc_top_changes.csv`
- `outputs_abc/abc_for_layout_candidates.csv`
- `outputs_abc/plots/*` si existe

## Cambio de carpeta base

Desde `Settings` puedes editar:

- ruta de `outputs`
- ruta de `outputs_basket`
- ruta de `outputs_abc`
- escenario por defecto
- rango por defecto
- modo oscuro

Tambien puedes usar el boton `Recargar datos` para limpiar cache y releer disco.

## Notas de entorno corporativo Windows

- No necesita `npm`, `node` ni backend adicional.
- Usa `Pathlib` y lectura local de CSV.
- Si falta un archivo, la app no se rompe: muestra aviso y sigue cargando los modulos disponibles.
- `rules_oper.csv` puede ser pesado; la app lo lee en chunks filtrados para evitar cargas completas cuando no haga falta.
- Los ficheros ABC se leen preservando SKU como texto para no perder ceros a la izquierda.

## Nuevo modulo: ABC Picking

Sirve para apoyar decisiones de layout y slotting con una vista Pareto por SKU basada en `pick_lines`, ahora extendida a `ABC-XYZ` y filtrado real por propietario.

Incluye:

- KPIs de concentracion de clase A.
- KPIs adicionales de `AX`, `% pick_lines AX` y SKUs volatiles `Z`.
- Pareto interactivo por vista `anual`, `trimestral` o `YTD`.
- Filtro `owner_scope` que trabaja sobre outputs recalculados por propietario.
- Modo `ABC` o `ABC-XYZ`.
- Scatter `mean_weekly_pick_lines` vs `cv_weekly` para separar rotacion y estabilidad.
- Tabla exportable con `pick_lines`, `pick_qty`, `n_orders`, acumulado, `xyz_class`, `abc_xyz_class` y recomendacion.
- Comparativa de cambios entre periodos (`A->B`, `B->A`, `AX->AZ`, etc.).
- Vista de recomendaciones operativas desde `abc_for_layout_candidates.csv`.

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
