# README_WEB

## Objetivo

Frontend React/Vite para visualizacion del forecast sin recalcular modelos.

Reglas:
- No tocar ni duplicar la logica del forecast engine.
- La web consume CSV ya generados.
- Fuente de datos de la web: `/data/*.csv`.

## Modo web dev (Mac / entorno libre)

Comandos:

```bash
cd web
npm install
npm run sync:data:sh
npm run dev
```

Que hace este flujo:
- `npm run sync:data:sh` copia desde `../outputs` hacia `web/public/data`.
- En Vite dev, `public/data` se sirve como `/data`.

## Modo web estatico (Windows / entorno restringido)

Objetivo: usar `web/dist` ya generado, sin npm.

Pasos:
1. Copiar la carpeta `web/dist` al equipo de trabajo.
2. Servir estatico:

```bash
python -m http.server 8080 --directory web/dist
```

3. Abrir en navegador `http://localhost:8080`.

Actualizar datos sin rebuild:
- Reemplazar CSV dentro de `web/dist/data`.
- Script recomendado en Windows:

```powershell
powershell -ExecutionPolicy Bypass -File web/scripts/sync_data_dist.ps1
```

## Scripts de sincronizacion

Se mantienen los scripts de desarrollo:
- `web/scripts/sync_data.sh`
- `web/scripts/sync_data.ps1`

Nuevos scripts para runtime estatico:
- `web/scripts/sync_data_dist.sh`
- `web/scripts/sync_data_dist.ps1`

Archivos sincronizados:
- `forecast_daily_business.csv`
- `forecast_weekly_business.csv`
- `backtest_metrics.csv`

Origen:
- `../outputs`

Destinos:
- Dev: `web/public/data`
- Estatico: `web/dist/data`

## npm audit: interpretacion correcta

Los warnings de `npm audit` no implican automaticamente fallo operativo del dashboard.

Guia:
1. Validar primero `npm run dev` y `npm run build`.
2. No ejecutar `npm audit fix --force` sin control.
3. Si se actualiza Vite u otras dependencias mayores, hacerlo con pruebas funcionales.

## Outputs minimos para la web

- `outputs/forecast_daily_business.csv`
- `outputs/forecast_weekly_business.csv`

Opcional para calidad de modelo en el dashboard:
- `outputs/backtest_metrics.csv`

Si falta algun CSV, la app usa mocks para no romper la interfaz.
