# README_WEB

## Objetivo

Frontend web React/Vite para uso en ordenador personal (Mac / entorno libre).

Importante:

- La web **NO** recalcula modelos.
- La web solo consume outputs ya generados por el forecast engine.

## Prerequisitos

- Node.js
- npm

## Flujo recomendado (Mac/personal)

Desde la raiz del repo:

```bash
cd web
npm install
npm run sync:data:sh
npm run dev
```

Antes de esto, si quieres datos actualizados, ejecuta primero el forecast engine en la raiz:

```bash
python main.py --horizon_days 60 --freq both --use_weather false
```

Que hace cada paso del frontend:

- `npm install`: instala dependencias del frontend.
- `npm run sync:data:sh`: copia CSV desde `../outputs/` hacia `web/public/data/`.
- `npm run dev`: arranca Vite en modo desarrollo.

## Script de sincronizacion de datos

Scripts disponibles:

- Mac/Linux: `npm run sync:data:sh`
- Windows PowerShell: `npm run sync:data`

Ambos scripts sincronizan estos archivos:

- `forecast_daily_business.csv`
- `forecast_weekly_business.csv`
- `backtest_metrics.csv`

Origen: `../outputs/`
Destino: `web/public/data/`

## Outputs que necesita la web

Minimo:

- `outputs/forecast_daily_business.csv`
- `outputs/forecast_weekly_business.csv`

Opcional (si quieres panel de calidad completo):

- `outputs/backtest_metrics.csv`

Si faltan CSV, la app usa datos mock para no romper la interfaz.

## npm audit: como interpretarlo

`npm audit` revisado el **2026-03-12** en este repo devuelve 2 vulnerabilidades moderadas encadenadas `vite -> esbuild`.
La correccion automatica propuesta sube a `vite@8` (cambio mayor).

Guia practica:

1. Primero prueba `npm run dev` y valida que el dashboard funcione.
2. No ejecutes `npm audit fix --force` automaticamente.
3. Si decides actualizar Vite, hazlo de forma controlada (upgrade, prueba de `dev`, prueba de `build`, validacion funcional).

Resumen: warnings de `npm audit` != fallo real inmediato del dashboard.

## Build de produccion (opcional)

```bash
cd web
npm run build
npm run preview
```

Nota: construir la web (`build`) tampoco recalcula forecast; solo empaqueta el frontend.

## Conexion con la arquitectura global

- Motor de forecast: `main.py` (genera outputs).
- Web: solo visualizacion de esos outputs.
- Streamlit: alternativa para entorno restringido en Windows.
