# Planificacion logistica: 1 forecast engine + 2 frontends

Este repositorio esta organizado como una arquitectura con **un solo motor de forecast** y **dos capas de visualizacion**.

## Arquitectura

### A) Forecast engine (fuente unica de verdad)

- `main.py`
- `src/`
- `data/`
- `outputs/`
- `outputs_basket/`
- `outputs_abc/`
- `basket_main.py` + `src_basket/` (analitica basket)
- `abc_main.py` + `src_abc/` (analitica ABC)

Regla clave:

- El forecast y las salidas de negocio se calculan en el engine.
- No hay pipelines duplicados.

### B) Frontend web (Mac / entorno libre)

- `web/`
- Solo lee CSV ya generados en `outputs/` (sin recalcular modelos)

### C) Frontend Streamlit (Windows / entorno restringido)

- `streamlit_app/`
- Solo lee CSV ya generados en `outputs/`, `outputs_basket/`, `outputs_abc/` (sin recalcular modelos)

## Cuando usar cada frontend

- Casa (Mac / entorno libre): usar **Web React/Vite**.
- Trabajo (Windows / restricciones corporativas): usar **Streamlit**.

## Flujo recomendado

1. Ejecutar el forecast engine para regenerar salidas.
2. Abrir el frontend que corresponda a tu entorno.

## Documentacion por modulo

- [README_FORECAST.md](README_FORECAST.md): pipeline principal, entradas, salidas y comandos.
- [README_WEB.md](README_WEB.md): frontend React/Vite para entorno personal.
- [README_STREAMLIT.md](README_STREAMLIT.md): frontend Streamlit para entorno corporativo.
- [README_BASKET.md](README_BASKET.md): modulo basket y outputs `outputs_basket/`.
- [README_ABC.md](README_ABC.md): modulo ABC-XYZ y outputs `outputs_abc/`.

## Regla de oro del repo

- **Modelos y pipeline: una sola vez en el engine**.
- **Visualizacion: dos frontends distintos consumiendo las mismas salidas**.
