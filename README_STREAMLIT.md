# README_STREAMLIT

## Objetivo

Frontend Streamlit para uso en ordenador de trabajo (Windows / entorno restringido).

Importante:

- Streamlit **NO** recalcula modelos.
- Streamlit solo visualiza outputs ya generados por el forecast engine.

## Prerequisitos

- Python
- `venv`
- Dependencias de `requirements_streamlit.txt`

## Arranque en Windows (entorno corporativo)

Si ya tienes `.venv` creado:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\activate
pip install -r requirements_streamlit.txt
streamlit run streamlit_app/app.py
```

Si no existe `.venv` todavia:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements_streamlit.txt
streamlit run streamlit_app/app.py
```

## Regenerar datos antes de abrir Streamlit

Ejecuta primero (en raiz del repo):

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\activate
python main.py --horizon_days 60 --freq both --use_weather false
```

Si quieres actualizar tambien modulos complementarios:

```powershell
python basket_main.py --input movimientos.xlsx --output_dir outputs_basket
python abc_main.py --input movimientos.xlsx --output_dir outputs_abc
```

## Sin Node ni npm

Este frontend funciona solo con Python + Streamlit.
No requiere Node, npm ni Vite.

## Rutas de outputs

Por defecto usa:

- `outputs/`
- `outputs_basket/`
- `outputs_abc/`

Puedes cambiarlas en la pagina **Settings** (`streamlit_app/pages/07_Settings.py`).

## Paginas actuales y datos que consumen

### 1) Resumen

- `outputs/forecast_daily_business.csv`
- `outputs/forecast_weekly_business.csv`

### 2) Transporte

- `outputs/forecast_daily_business.csv`
- `outputs/forecast_weekly_business.csv`

### 3) Picking

- `outputs/forecast_daily_business.csv`
- `outputs/forecast_weekly_business.csv`

### 4) Calidad del modelo

- `outputs/backtest_metrics.csv`
- `outputs/model_registry.csv`
- `outputs/join_kpis.csv`

### 5) Optimizacion picking

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
- `outputs_basket/sku_neighbors.csv` (opcional)
- `outputs_basket/plots/*` (opcional)

### 6) ABC Picking

- `outputs_abc/abc_picking_annual.csv`
- `outputs_abc/abc_picking_quarterly.csv`
- `outputs_abc/abc_picking_ytd.csv`
- `outputs_abc/abc_summary_by_period.csv`
- `outputs_abc/abc_xyz_summary_by_period.csv`
- `outputs_abc/abc_owner_summary.csv`
- `outputs_abc/abc_top_changes.csv`
- `outputs_abc/abc_for_layout_candidates.csv`
- `outputs_abc/plots/*` (opcional)

### 7) Settings

- Configuracion de rutas base
- Defaults de escenario/rango
- Estado de disponibilidad de archivos
- Boton de recarga de cache

## Conexion con la arquitectura global

- Motor de forecast: produce CSV en `outputs/`, `outputs_basket/`, `outputs_abc/`.
- Streamlit: capa de visualizacion para entorno restringido.
- Web React/Vite: alternativa para entorno libre en Mac.
