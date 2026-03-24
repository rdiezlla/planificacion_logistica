# Planificacion logistica

Proyecto orientado a un flujo unico:

1. Forecast engine (`main.py` + `src/`)
2. Frontend principal web React/Vite (`web/`)

La primera pagina web de supervisor se simplifica a 3 series por metrica:
- Forecast
- Ano 2024 (comparativo)
- Real observado 2026

## Arquitectura

- Motor forecast: `main.py`, `src/`
- Capa canonica de servicios: `data/processed/fact_services_canonical.parquet`
- Outputs supervisor:
  - `outputs/supervisor_dashboard_daily.csv`
  - `outputs/supervisor_dashboard_weekly.csv`
- Historial de snapshots:
  - `outputs/history/supervisor_snapshots/`
  - `outputs/forecast_snapshot_registry.csv`
  - `outputs/supervisor_forecast_history.csv`
- Frontend web: `web/`

## Ejecutar pipeline

```bash
python main.py --horizon_days 60 --freq both --data_mode hybrid --operational_cutover_date 2026-03-01
python run_daily_pipeline.py
```

Nota:

- El forecast ya no utiliza meteo.
- Las fuentes de OneDrive se resuelven priorizando `pruebas/Descargas BI/`.

## Web en desarrollo (Mac / entorno libre)

```bash
cd web
npm install
npm run sync:data:sh
npm run dev
```

## Web estatica (Windows / entorno restringido)

```bash
python -m http.server 8080 --directory web/dist
```

Abrir `http://localhost:8080`.

Refrescar CSV de `web/dist/data` sin rebuild:

```powershell
powershell -ExecutionPolicy Bypass -File web/scripts/sync_data_dist.ps1
```

## Documentacion

- [README_FORECAST.md](README_FORECAST.md)
- [README_WORKFLOW.md](README_WORKFLOW.md)
- [README_WEB.md](README_WEB.md)
- [README_SUPERVISOR_DASHBOARD.md](README_SUPERVISOR_DASHBOARD.md)
- [README_BASKET.md](README_BASKET.md)
- [README_ABC.md](README_ABC.md)
