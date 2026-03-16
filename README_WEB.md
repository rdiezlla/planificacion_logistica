# README_WEB

## Frontend principal

La web React/Vite es el frontend unico del proyecto.

Primera pagina real: `Resumen Supervisor`.

## Semantica visual del supervisor

Cada grafico muestra 3 lineas:

1. Forecast
2. Ano 2024
3. Real 2026 observado

KPIs superiores:
- valor principal: forecast semana activa
- delta principal: forecast vs 2024 comparable
- texto secundario: real 2026 observado vs 2024 (si aplica)

## Contrato de datos

Fuente principal:
- `/data/supervisor_dashboard_weekly.csv`

Fuente de apoyo/filtro:
- `/data/supervisor_dashboard_daily.csv`

Fallback:
- si faltan CSV, la app usa mocks.

## Desarrollo (Mac)

```bash
cd web
npm install
npm run sync:data:sh
npm run dev
```

## Build

```bash
cd web
npm run build
npm run preview
```

## Runtime estatico (Windows / sin npm)

```bash
python -m http.server 8080 --directory web/dist
```

Abrir:

`http://localhost:8080`

Actualizar datos sin rebuild:

```powershell
powershell -ExecutionPolicy Bypass -File web/scripts/sync_data_dist.ps1
```

## Scripts de sync

Desarrollo:
- `web/scripts/sync_data.sh`
- `web/scripts/sync_data.ps1`

Runtime estatico:
- `web/scripts/sync_data_dist.sh`
- `web/scripts/sync_data_dist.ps1`

CSV sincronizados:
- `forecast_daily_business.csv`
- `forecast_weekly_business.csv`
- `backtest_metrics.csv`
- `supervisor_dashboard_daily.csv`
- `supervisor_dashboard_weekly.csv`

Origen:
- `../outputs`

Destino:
- dev: `web/public/data`
- estatico: `web/dist/data`

## npm audit

Warnings de `npm audit` no implican automaticamente fallo operativo.

Recomendacion:
1. validar `npm run dev` y `npm run build`
2. no usar `npm audit fix --force` sin validar impacto
