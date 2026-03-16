# README_SUPERVISOR_DASHBOARD

## Alcance de la primera pagina

`Resumen Supervisor` se centra en 3 metricas:

1. Numero de salidas
2. Numero de recogidas
3. Numero de lineas de picking esperadas

Y 3 series por metrica:

- Forecast
- Ano 2024
- Real observado 2026

## Origen de datos

- `outputs/supervisor_dashboard_daily.csv`
- `outputs/supervisor_dashboard_weekly.csv`

Generacion:
- `main.py`
- soporte de series en `src/supervisor_dashboard.py`

## Definicion de campos (weekly)

- `salidas_forecast`, `salidas_2024`, `salidas_real_2026`
- `recogidas_forecast`, `recogidas_2024`, `recogidas_real_2026`
- `pick_lines_forecast`, `pick_lines_2024`, `pick_lines_real_2026`
- `week_iso`, `week_start_date`, `week_end_date`
- `cutoff_date`
- `forecast_snapshot_date`
- `year_target`
- `comparison_year`

## KPI cards

Cada tarjeta muestra:

- forecast de la semana activa
- delta forecast vs 2024 comparable
- referencia de real 2026 observado frente a 2024 (si hay observacion)

## Snapshots

Cada ejecucion guarda snapshots del forecast supervisor en:

- `outputs/history/supervisor_snapshots/`

Adicionalmente:

- `outputs/forecast_snapshot_registry.csv`
- `outputs/supervisor_forecast_history.csv`

Esto permite comparar forecast emitido en una fecha contra real final.
