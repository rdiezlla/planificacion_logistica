# Web Dashboard

Dashboard base para transporte y almacen con modo estatico por defecto.

## Stack

- React + Vite + TypeScript
- Tailwind
- Recharts
- React Router

No depende de backend para funcionar.
Usa `HashRouter`, asi que tambien puede publicarse como estatico puro sin configurar rewrites.

## Modo corporativo

1. `cd web`
2. `npm install`
3. `npm run sync:data`
4. `npm run dev`

Si en tu equipo corporativo no puedes instalar dependencias o no tienes Node, genera el build en una maquina personal y copia `web/dist/` dentro de `web/dist-demo/`.

## Modo personal

1. `cd web`
2. `npm install`
3. `npm run sync:data`
4. `npm run dev`

Build de produccion:

1. `npm run build`
2. servir `web/dist/` con cualquier servidor estatico

## Datos que consume

Desde `web/public/data/`:

- `forecast_weekly_business.csv`
- `forecast_daily_business.csv`
- `backtest_metrics.csv` opcional

## Refresco de datos

Script Windows:

```powershell
cd web
npm run sync:data
```

Script Mac/Linux:

```bash
cd web
npm run sync:data:sh
```

Los scripts copian automaticamente desde `../outputs` hacia `web/public/data`.

## Lectura de negocio

- Transporte: usar OUT e IN desde `forecast_weekly_business.csv` y `forecast_daily_business.csv`.
- Almacen: usar `picking_movs_esperados_p50/p80`, que representa picking esperado por fecha de preparacion.
- `P50`: caso base operativo.
- `P80`: caso alto realista para tensionar capacidad.

## Backend opcional

Existe un backend opcional en `web/server/` con Express para servir los CSV desde `../outputs`, pero el frontend no lo necesita.

Arranque opcional:

```bash
cd web/server
npm install
npm start
```
