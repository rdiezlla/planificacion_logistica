from __future__ import annotations

from pathlib import Path

import streamlit as st

from streamlit_app.utils.business_logic import RangeSelection


DEFAULTS = {
    "base_outputs_dir": "",
    "base_outputs_basket_dir": "",
    "default_scenario": "P50",
    "default_range": "12 semanas",
    "dark_mode": False,
}


def init_app_state(project_root: Path) -> None:
    st.session_state.setdefault("base_outputs_dir", str(project_root / "outputs"))
    st.session_state.setdefault("base_outputs_basket_dir", str(project_root / "outputs_basket"))
    st.session_state.setdefault("default_scenario", DEFAULTS["default_scenario"])
    st.session_state.setdefault("default_range", DEFAULTS["default_range"])
    st.session_state.setdefault("dark_mode", DEFAULTS["dark_mode"])


def render_global_sidebar(
    title: str,
    *,
    show_scenario: bool = True,
    show_frequency: bool = True,
    show_range: bool = True,
) -> dict[str, object]:
    result: dict[str, object] = {}
    with st.sidebar:
        st.markdown(f"### {title}")
        if show_scenario:
            result["scenario"] = st.selectbox(
                "Escenario",
                ["P50", "P80"],
                index=0 if st.session_state.get("default_scenario", "P50") == "P50" else 1,
                key=f"scenario_{title}",
            ).lower()
        if show_frequency:
            result["frequency"] = st.selectbox(
                "Granularidad",
                ["weekly", "daily"],
                index=0,
                format_func=lambda v: "Semanal" if v == "weekly" else "Diaria",
                key=f"freq_{title}",
            )
        range_preset = st.session_state.get("default_range", "12 semanas")
        custom_start = None
        custom_end = None
        if show_range:
            range_preset = st.selectbox(
                "Rango temporal",
                ["8 semanas", "12 semanas", "26 semanas", "custom"],
                index=["8 semanas", "12 semanas", "26 semanas", "custom"].index(range_preset),
                key=f"range_{title}",
            )
            if range_preset == "custom":
                custom_start = st.date_input("Desde", key=f"from_{title}")
                custom_end = st.date_input("Hasta", key=f"to_{title}")
            result["range"] = RangeSelection(range_preset, custom_start, custom_end)
    return result
