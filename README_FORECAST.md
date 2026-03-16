# README_FORECAST

## Objetivo actual

Mantener el motor de forecast estable y simplificar la capa visible del dashboard supervisor.

La vista supervisor ya no se explica como `model + committed + hybrid`.
Ahora consume series directas y claras:

- `forecast`
- `2024` (comparativo historico)
- `real_2026` (observado)

## Estructura operativa

- Pipeline principal: `main.py`
- Modulos: `src/`
- Capa canonica:
  - `data/processed/stg_services_legacy.parquet`
  - `data/processed/stg_services_operational.parquet`
  - `data/processed/fact_services_canonical.parquet`
- Auditoria de fuentes:
  - `outputs/pedidos_source_audit.csv`

## Modos de dato del pipeline

`main.py` mantiene soporte:

- `--data_mode legacy`
- `--data_mode hybrid`
- `--data_mode operational-first`

Cutover operativo:

- `--operational_cutover_date 2026-03-01`

## Outputs supervisor (simplificados)

### `outputs/supervisor_dashboard_weekly.csv`

Campos principales:

- `week_iso`
- `week_start_date`
- `week_end_date`
- `salidas_forecast`
- `salidas_2024`
- `salidas_real_2026`
- `recogidas_forecast`
- `recogidas_2024`
- `recogidas_real_2026`
- `pick_lines_forecast`
- `pick_lines_2024`
- `pick_lines_real_2026`
- `cutoff_date`
- `forecast_snapshot_date`
- `year_target` (2026)
- `comparison_year` (2024)

### `outputs/supervisor_dashboard_daily.csv`

Misma semantica por fecha (incluye metadatos de semana y filtros diarios).

## Snapshots de forecast supervisor

Cada ejecucion del pipeline guarda snapshots:

- `outputs/history/supervisor_snapshots/supervisor_dashboard_weekly__YYYY-MM-DD.csv`
- `outputs/history/supervisor_snapshots/supervisor_dashboard_daily__YYYY-MM-DD.csv`

Registro:

- `outputs/forecast_snapshot_registry.csv`
  - `snapshot_date`
  - `cutoff_date`
  - `file_name`
  - `rows`
  - `created_ts`

Historico consolidado:

- `outputs/supervisor_forecast_history.csv`
  - `snapshot_date`
  - `grain` (`daily|weekly`)
  - `metric`
  - `week_iso` / `fecha`
  - `forecast_value`

## Regla de picking

- Picking esperado se mantiene ligado a salidas.
- Recogidas no generan picking.

## Ejecucion recomendada

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py --horizon_days 60 --freq both --use_weather false --data_mode hybrid --operational_cutover_date 2026-03-01
```
