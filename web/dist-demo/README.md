# dist-demo

En este equipo no hay `node/npm`, asi que no he podido generar el build final aqui.

Uso recomendado en una maquina personal:

```bash
cd web
npm install
npm run sync:data
npm run build
```

Despues copia el contenido generado en `web/dist/` a esta carpeta `web/dist-demo/` si necesitas llevar un build cerrado al PC corporativo.

La app esta preparada para funcionar en modo estatico leyendo:

- `/data/forecast_weekly_business.csv`
- `/data/forecast_daily_business.csv`
- `/data/backtest_metrics.csv`

Si esos CSV no existen, el frontend usa mocks internos.
