from __future__ import annotations

import streamlit as st


def clear_all_caches() -> None:
    st.cache_data.clear()
    st.cache_resource.clear()
