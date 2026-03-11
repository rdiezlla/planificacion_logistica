from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from streamlit_app.components.cards import render_kpi_cards
from streamlit_app.components.charts import IN_COLOR, OUT_COLOR, bar_chart, line_hist_forecast
from streamlit_app.components.filters import render_global_sidebar
from streamlit_app.components.layout import card_title, render_page_header, show_missing_file
from streamlit_app.components.loaders import load_forecast_bundle
from streamlit_app.utils.business_logic import filter_range, period_mask
from streamlit_app.utils.formatters import fmt_int, fmt_number

render_page_header(
    "Transporte",
    "Detalle de entregas y recogidas para planificacion de transporte y lectura de facturacion.",
    eyebrow="Transport planning",
)
filters = render_global_sidebar("Transporte")
bundle = load_forecast_bundle(st.session_state["base_outputs_dir"])

freq = filters.get("frequency", "weekly")
scenario = filters.get("scenario", "p50")
source_key = "forecast_weekly_business" if freq == "weekly" else "forecast_daily_business"
df = bundle["data"].get(source_key)
if df is None or df.empty:
    show_missing_file(source_key, bundle["files"][source_key]["path"])
    st.stop()

df = filter_range(df, freq, filters["range"])
date_col = "week_start_date" if freq == "weekly" else "fecha"
df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
mask_hist = period_mask(df, freq)
suffix = "_semana" if freq == "weekly" else ""

tab_out, tab_in, tab_compare = st.tabs(["Entregas OUT", "Recogidas IN", "Comparativa"])

with tab_out:
    cols_map = {
        "Eventos": f"eventos_entrega{suffix}_{scenario}",
        "M3": f"m3_out{suffix}_{scenario}",
        "Pales": f"pales_out{suffix}_{scenario}",
        "Cajas": f"cajas_out{suffix}_{scenario}",
        "Peso facturable": f"peso_facturable_out{suffix}_{scenario}",
    }
    cards = [
        {"label": label, "value": fmt_int(df[col].iloc[-1]) if label in {"Eventos", "Pales", "Cajas"} else fmt_number(df[col].iloc[-1], 1)}
        for label, col in cols_map.items() if col in df.columns
    ]
    render_kpi_cards(cards, columns=5)
    for left, right in [("Eventos", "M3"), ("Pales", "Cajas")]:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="app-card">', unsafe_allow_html=True)
            card_title(left)
            st.plotly_chart(line_hist_forecast(df, date_col, [cols_map[left]], [left], [OUT_COLOR], mask_hist), use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with col2:
            st.markdown('<div class="app-card">', unsafe_allow_html=True)
            card_title(right)
            st.plotly_chart(line_hist_forecast(df, date_col, [cols_map[right]], [right], [OUT_COLOR], mask_hist), use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
    visible_cols = [date_col] + [col for col in cols_map.values() if col in df.columns]
    st.dataframe(df[visible_cols], use_container_width=True, hide_index=True)
    st.download_button("Descargar OUT filtrado", df[visible_cols].to_csv(index=False).encode("utf-8"), file_name=f"transporte_out_{freq}_{scenario}.csv", mime="text/csv")

with tab_in:
    cols_map = {
        "Eventos": f"eventos_recogida{suffix}_{scenario}",
        "M3": f"m3_in{suffix}_{scenario}",
        "Pales": f"pales_in{suffix}_{scenario}",
        "Cajas": f"cajas_in{suffix}_{scenario}",
        "Peso facturable": f"peso_facturable_in{suffix}_{scenario}",
    }
    cards = [
        {"label": label, "value": fmt_int(df[col].iloc[-1]) if label in {"Eventos", "Pales", "Cajas"} else fmt_number(df[col].iloc[-1], 1)}
        for label, col in cols_map.items() if col in df.columns
    ]
    render_kpi_cards(cards, columns=5)
    for left, right in [("Eventos", "M3"), ("Pales", "Cajas")]:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="app-card">', unsafe_allow_html=True)
            card_title(left)
            st.plotly_chart(line_hist_forecast(df, date_col, [cols_map[left]], [left], [IN_COLOR], mask_hist), use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with col2:
            st.markdown('<div class="app-card">', unsafe_allow_html=True)
            card_title(right)
            st.plotly_chart(line_hist_forecast(df, date_col, [cols_map[right]], [right], [IN_COLOR], mask_hist), use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
    visible_cols = [date_col] + [col for col in cols_map.values() if col in df.columns]
    st.dataframe(df[visible_cols], use_container_width=True, hide_index=True)
    st.download_button("Descargar IN filtrado", df[visible_cols].to_csv(index=False).encode("utf-8"), file_name=f"transporte_in_{freq}_{scenario}.csv", mime="text/csv")

with tab_compare:
    compare_df = pd.DataFrame(
        {
            date_col: df[date_col],
            "OUT eventos": df[f"eventos_entrega{suffix}_{scenario}"],
            "IN eventos": df[f"eventos_recogida{suffix}_{scenario}"],
            "OUT m3": df[f"m3_out{suffix}_{scenario}"],
            "IN m3": df[f"m3_in{suffix}_{scenario}"],
        }
    )
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Comparativa OUT vs IN")
    fig = line_hist_forecast(compare_df, date_col, ["OUT eventos", "IN eventos", "OUT m3", "IN m3"], ["OUT eventos", "IN eventos", "OUT m3", "IN m3"], [OUT_COLOR, IN_COLOR, "#1d4ed8", "#f97316"], mask_hist)
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)
    summary = compare_df.tail(12).copy()
    summary[date_col] = summary[date_col].dt.strftime("%d/%m/%Y")
    st.plotly_chart(bar_chart(summary, x=date_col, y="OUT eventos", color=OUT_COLOR), use_container_width=True)
