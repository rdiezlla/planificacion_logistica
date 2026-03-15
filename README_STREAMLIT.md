# README_STREAMLIT

## Objetivo

Streamlit es la alternativa de frontend para entorno capado (Windows/restringido), sin npm y sin Vite.

Reglas:
- No recalcula modelos.
- Consume los mismos outputs del engine.

## Arranque rapido

Comando principal:

```bash
streamlit run streamlit_app/app.py
```

## Windows (entorno corporativo)

Si no existe entorno virtual:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements_streamlit.txt
streamlit run streamlit_app/app.py
```

Si `.venv` ya existe:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\activate
pip install -r requirements_streamlit.txt
streamlit run streamlit_app/app.py
```

## Datos que consume

Por defecto:
- `outputs/`
- `outputs_basket/`
- `outputs_abc/`

Estas rutas se pueden ajustar desde la pagina Settings de Streamlit.

## Relacion con la arquitectura

- Unico forecast engine: genera CSV en `outputs*`.
- Frontend web React: opcion para entorno libre.
- Frontend Streamlit: opcion para entorno capado.
- Ambos frontends consumen salidas, no entrenan ni recalculan modelos.
