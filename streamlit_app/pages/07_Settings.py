from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from streamlit_app.components.layout import card_title, render_page_header
from streamlit_app.components.loaders import ABC_FILES, BASKET_FILES, FORECAST_FILES
from streamlit_app.utils.cache import clear_all_caches
from streamlit_app.utils.formatters import fmt_date, fmt_number
from streamlit_app.utils.io import file_info

render_page_header(
    "Settings",
    "Configuracion de rutas base, defaults de visualizacion y estado de disponibilidad de ficheros.",
    eyebrow="Configuration",
)

with st.form("settings_form"):
    outputs_dir = st.text_input("Carpeta outputs", value=st.session_state["base_outputs_dir"])
    outputs_basket_dir = st.text_input("Carpeta outputs_basket", value=st.session_state["base_outputs_basket_dir"])
    outputs_abc_dir = st.text_input("Carpeta outputs_abc", value=st.session_state["base_outputs_abc_dir"])
    default_scenario = st.selectbox("Escenario por defecto", ["P50", "P80"], index=0 if st.session_state.get("default_scenario", "P50") == "P50" else 1)
    default_range = st.selectbox("Rango por defecto", ["8 semanas", "12 semanas", "26 semanas", "custom"], index=["8 semanas", "12 semanas", "26 semanas", "custom"].index(st.session_state.get("default_range", "12 semanas")))
    dark_mode = st.checkbox("Modo oscuro", value=bool(st.session_state.get("dark_mode", False)))
    submitted = st.form_submit_button("Guardar configuracion")

if submitted:
    st.session_state["base_outputs_dir"] = outputs_dir
    st.session_state["base_outputs_basket_dir"] = outputs_basket_dir
    st.session_state["base_outputs_abc_dir"] = outputs_abc_dir
    st.session_state["default_scenario"] = default_scenario
    st.session_state["default_range"] = default_range
    st.session_state["dark_mode"] = dark_mode
    st.success("Configuracion actualizada. Si cambiaste modo visual, recarga la pagina.")

col1, col2 = st.columns([1.0, 1.0])
with col1:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Acciones")
    if st.button("Recargar datos"):
        clear_all_caches()
        st.success("Caches limpiadas. La siguiente lectura usara disco.")
    st.markdown("</div>", unsafe_allow_html=True)

def _build_status_table(base_dir: str, files_map: dict[str, str]) -> pd.DataFrame:
    rows = []
    for key, filename in files_map.items():
        info = file_info(Path(base_dir) / filename)
        rows.append(
            {
                "dataset": key,
                "archivo": filename,
                "disponible": info["exists"],
                "modificado": fmt_date(info["modified"]),
                "tamano_mb": fmt_number(info["size_mb"], 2) if info["size_mb"] is not None else "-",
            }
        )
    return pd.DataFrame(rows)


forecast_status = _build_status_table(st.session_state["base_outputs_dir"], FORECAST_FILES)
basket_status = _build_status_table(st.session_state["base_outputs_basket_dir"], BASKET_FILES)
abc_status = _build_status_table(st.session_state["base_outputs_abc_dir"], ABC_FILES)
all_modified = pd.to_datetime(
    pd.concat([forecast_status["modificado"], basket_status["modificado"], abc_status["modificado"]], ignore_index=True),
    format="%d/%m/%Y",
    errors="coerce",
)
last_modified = all_modified.max()

with col2:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Carpetas activas")
    st.write(f"`outputs`: {st.session_state['base_outputs_dir']}")
    st.write(f"`outputs_basket`: {st.session_state['base_outputs_basket_dir']}")
    st.write(f"`outputs_abc`: {st.session_state['base_outputs_abc_dir']}")
    st.write(f"Ultima fecha detectada: {fmt_date(last_modified)}")
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="app-card">', unsafe_allow_html=True)
card_title("Estado de archivos - forecast")
st.dataframe(forecast_status, use_container_width=True, hide_index=True)
st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="app-card">', unsafe_allow_html=True)
card_title("Estado de archivos - basket")
st.dataframe(basket_status, use_container_width=True, hide_index=True)
st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="app-card">', unsafe_allow_html=True)
card_title("Estado de archivos - ABC")
st.dataframe(abc_status, use_container_width=True, hide_index=True)
st.markdown("</div>", unsafe_allow_html=True)
