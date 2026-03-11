from __future__ import annotations

from pathlib import Path
import sys

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from streamlit_app.components.filters import init_app_state
from streamlit_app.components.styles import apply_global_styles

APP_DIR = Path(__file__).resolve().parent

st.set_page_config(
    page_title="Planning Hub",
    page_icon=":material/stacked_line_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)

init_app_state(PROJECT_ROOT)
apply_global_styles(dark_mode=bool(st.session_state.get("dark_mode", False)))

pages = [
    st.Page(str(APP_DIR / "pages" / "01_Resumen.py"), title="Resumen", icon=":material/dashboard:"),
    st.Page(str(APP_DIR / "pages" / "02_Transporte.py"), title="Transporte", icon=":material/local_shipping:"),
    st.Page(str(APP_DIR / "pages" / "03_Picking.py"), title="Picking", icon=":material/inventory_2:"),
    st.Page(str(APP_DIR / "pages" / "04_Calidad_modelo.py"), title="Calidad del modelo", icon=":material/query_stats:"),
    st.Page(str(APP_DIR / "pages" / "05_Optimizacion_picking.py"), title="Optimizacion picking", icon=":material/hub:"),
    st.Page(str(APP_DIR / "pages" / "06_ABC_Picking.py"), title="ABC Picking", icon=":material/stacked_bar_chart:"),
    st.Page(str(APP_DIR / "pages" / "07_Settings.py"), title="Settings", icon=":material/settings:"),
]

navigation = st.navigation(pages, position="sidebar")
navigation.run()
