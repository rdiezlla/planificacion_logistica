# Workflow Diario

Este documento resume el flujo real con la estructura nueva de OneDrive.

## Donde vive cada cosa

- OneDrive `pruebas/Descargas BI/Informacion_albaranaes.xlsx`
- OneDrive `pruebas/Descargas BI/movimientos.xlsx`
- OneDrive `pruebas/lineas_solicitudes_con_pedidos.xlsx`
- OneDrive `pruebas/maestro_dimensiones_limpio.xlsx`
- OneDrive `pruebas/Descargas BI/Movimientos/`
- OneDrive `pruebas/Descargas BI/Movimientos pedidos/`
- OneDrive `pruebas/Descargas BI/limpieza_movimientos.ipynb`
- OneDrive `pruebas/Descargas BI/limpieza_pedidos.ipynb`
- OneDrive `pruebas/limpieza_general.py`
- Repo `outputs/`, `outputs_abc/`, `outputs_basket/`

## Flujo completo

```mermaid
flowchart TD
    A["SGA / descargas crudas"] --> B["Descargas BI/Movimientos"]
    A --> C["Descargas BI/Movimientos pedidos"]
    B --> D["limpieza_movimientos.ipynb"]
    C --> E["limpieza_pedidos.ipynb o limpieza_general.py"]
    D --> F["Descargas BI/movimientos.xlsx"]
    E --> G["pruebas/lineas_solicitudes_con_pedidos.xlsx"]
    H["Descargas BI/Informacion_albaranaes.xlsx"] --> I["Forecast main.py"]
    F --> I
    G --> I
    J["pruebas/maestro_dimensiones_limpio.xlsx"] --> I
    F --> K["ABC abc_main.py"]
    F --> L["Basket basket_main.py"]
    I --> M["outputs/"]
    K --> N["outputs_abc/"]
    L --> O["outputs_basket/"]
    M --> P["Streamlit / Web"]
    N --> P
    O --> P
```

## Limpieza de movimientos

```mermaid
flowchart LR
    A["Descargas BI/Movimientos/*.xls"] --> B["Notebook limpieza_movimientos"]
    B --> C["Unificacion de periodos"]
    C --> D["Estandarizacion de columnas"]
    D --> E["Parse de fechas"]
    E --> F["Normalizacion pedido / pedido_externo"]
    F --> G["movimientos.xlsx consolidado"]
    G --> H["main.py / abc_main.py / basket_main.py"]
```

## Limpieza de pedidos

```mermaid
flowchart LR
    A["Descargas BI/Movimientos pedidos/*.xls"] --> B["Notebook limpieza_pedidos o limpieza_general.py"]
    C["Descargas BI/MAHOU_-_*.csv"] --> B
    D["maestro_dimensiones_limpio.xlsx"] --> B
    B --> E["Cruce lineas + pedidos + maestro"]
    E --> F["Correccion de propietario / ubicacion"]
    F --> G["lineas_solicitudes_con_pedidos.xlsx"]
    G --> H["main.py"]
```

## Forecast

```mermaid
flowchart TD
    A["Informacion_albaranaes.xlsx"] --> B["clean_albaranes"]
    C["movimientos.xlsx"] --> D["clean_movimientos"]
    E["lineas_solicitudes_con_pedidos.xlsx"] --> F["capa operacional"]
    B --> G["capa canonica de servicios"]
    F --> G
    D --> H["join movimientos -> servicios"]
    G --> I["targets service por fecha_servicio"]
    D --> J["targets workload por fecha_inicio"]
    G --> K["expected workload desde servicio"]
    I --> L["feature engineering calendario + festivos + semana santa + lags"]
    J --> L
    K --> M["staffing"]
    L --> N["train / backtest / forecast"]
    N --> O["outputs negocio"]
    H --> O
    K --> O
    M --> O
```

## ABC Picking

```mermaid
flowchart LR
    A["movimientos.xlsx"] --> B["Filtrar Tipo movimiento = PI"]
    B --> C["Agrupar por SKU y periodo"]
    C --> D["pick_lines / pick_qty / n_orders / n_days_active"]
    D --> E["Orden descendente por pick_lines"]
    E --> F["Pareto puro abc_pareto_class"]
    F --> G["Suelo operativo abc_class"]
    G --> H["ABC-XYZ + layout candidates"]
    H --> I["outputs_abc/ + Streamlit"]
```

## Basket Picking

```mermaid
flowchart LR
    A["movimientos.xlsx"] --> B["Filtrar PI"]
    B --> C["Transaccion operativa = pedido_externo x propietario"]
    B --> D["Transaccion pedido = pedido_externo"]
    C --> E["coocurrencias / reglas / clusters"]
    D --> F["comparativa order vs oper"]
    B --> G["penalizacion multi-propietario"]
    E --> H["outputs_basket/"]
    F --> H
    G --> H
```

## Comando diario recomendado

Sin refrescar inputs limpios:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\activate
python run_daily_pipeline.py
```

Refrescando primero los inputs limpios con el script externo si esta disponible:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\activate
python run_daily_pipeline.py --refresh_clean_inputs
```

Wrapper PowerShell:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\scripts\run_daily_pipeline.ps1
```

## Logica operativa recomendada

1. Actualizar descargas en `Descargas BI`.
2. Regenerar `movimientos.xlsx` y `lineas_solicitudes_con_pedidos.xlsx` si hubo cambios crudos.
3. Lanzar `run_daily_pipeline.py`.
4. Revisar `outputs/`, `outputs_abc/` y `outputs_basket/`.
5. Abrir Streamlit o web para consumir resultados.
