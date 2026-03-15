# Planificacion logistica

Arquitectura objetivo del repo:
- Un unico forecast engine.
- Dos frontends que consumen los mismos outputs.

## Arquitectura

### 1) Forecast engine (fuente unica de verdad)

Componentes:
- `main.py`
- `src/`
- `basket_main.py` + `src_basket/`
- `abc_main.py` + `src_abc/`

Salidas:
- `outputs/`
- `outputs_basket/`
- `outputs_abc/`

Regla clave:
- No duplicar modelos ni logica de forecast en frontends.

### 2) Frontend web React/Vite

Carpeta:
- `web/`

Modo dev (Mac / entorno libre):

```bash
cd web
npm install
npm run sync:data:sh
npm run dev
```

Modo estatico (Windows / entorno restringido, sin npm):

```bash
python -m http.server 8080 --directory web/dist
```

Abrir en navegador:

```text
http://localhost:8080
```

Refresco de datos en modo estatico:

```powershell
powershell -ExecutionPolicy Bypass -File web/scripts/sync_data_dist.ps1
```

### 3) Frontend Streamlit

Carpeta:
- `streamlit_app/`

Comando:

```bash
streamlit run streamlit_app/app.py
```

## Que frontend usar

- Entorno libre/personal: Web React/Vite.
- Entorno capado/trabajo: Web estatica (`web/dist`) o Streamlit.

## Documentacion por modulo

- [README_FORECAST.md](README_FORECAST.md)
- [README_WEB.md](README_WEB.md)
- [README_STREAMLIT.md](README_STREAMLIT.md)
- [README_BASKET.md](README_BASKET.md)
- [README_ABC.md](README_ABC.md)
