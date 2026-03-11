from __future__ import annotations

import streamlit as st


def render_page_header(title: str, subtitle: str, eyebrow: str = "Logistics dashboard") -> None:
    st.markdown(
        f"""
        <div class="app-header">
            <div class="app-eyebrow">{eyebrow}</div>
            <div class="app-title">{title}</div>
            <p class="app-subtitle">{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def card_title(title: str, hint: str | None = None) -> None:
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    if hint:
        st.markdown(f'<div class="hint">{hint}</div>', unsafe_allow_html=True)


def show_missing_file(label: str, path: str) -> None:
    st.info(f"{label}: archivo no disponible todavia. Ruta esperada: `{path}`")


def show_note(text: str) -> None:
    st.caption(text)
