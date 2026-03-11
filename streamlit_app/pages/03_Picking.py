from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from streamlit_app.components.cards import render_kpi_cards
from streamlit_app.components.charts import PICKING_COLOR, bar_chart, line_hist_forecast
from streamlit_app.components.filters import render_global_sidebar
from streamlit_app.components.layout import card_title, render_page_header, show_missing_file
from streamlit_app.components.loaders import load_forecast_bundle
from streamlit_app.utils.business_logic import filter_range, period_mask
from streamlit_app.utils.formatters import fmt_int

render_page_header(
    "Picking",
    "Planificacion del picking esperado en almacen. Solo aplica a entregas y se desplaza por lead time y urgencia.",
    eyebrow="Warehouse planning",
)
filters = render_global_sidebar("Picking")
bundle = load_forecast_bundle(st.session_state["base_outputs_dir"])

weekly = bundle["data"].get("forecast_weekly_business")
daily = bundle["data"].get("forecast_daily_business")
if weekly is None or weekly.empty:
    show_missing_file("forecast_weekly_business", bundle["files"]["forecast_weekly_business"]["path"])
    st.stop()
if daily is None or daily.empty:
    show_missing_file("forecast_daily_business", bundle["files"]["forecast_daily_business"]["path"])

scenario = filters.get("scenario", "p50")
freq = filters.get("frequency", "weekly")

weekly = filter_range(weekly, "weekly", filters["range"])
weekly["week_start_date"] = pd.to_datetime(weekly["week_start_date"], errors="coerce")
mask_hist_week = period_mask(weekly, "weekly")
pick_col_week = f"picking_movs_esperados_semana_{scenario}"

cards = [
    {"label": "Picking esperado semana", "value": fmt_int(weekly[pick_col_week].iloc[-1])},
    {"label": "Picking P50 semana", "value": fmt_int(weekly["picking_movs_esperados_semana_p50"].iloc[-1])},
    {"label": "Picking P80 semana", "value": fmt_int(weekly["picking_movs_esperados_semana_p80"].iloc[-1])},
]
if "picking_movs_reales_semana" in weekly.columns:
    cards.append({"label": "Picking real semana", "value": fmt_int(weekly["picking_movs_reales_semana"].iloc[-1])})
render_kpi_cards(cards, columns=4)

st.markdown('<div class="app-card">', unsafe_allow_html=True)
card_title("Serie semanal")
weekly_fig = line_hist_forecast(
    weekly,
    "week_start_date",
    [pick_col_week, "picking_movs_reales_semana"] if "picking_movs_reales_semana" in weekly.columns else [pick_col_week],
    ["Picking esperado", "Picking real"] if "picking_movs_reales_semana" in weekly.columns else ["Picking esperado"],
    [PICKING_COLOR, "#0f172a"] if "picking_movs_reales_semana" in weekly.columns else [PICKING_COLOR],
    mask_hist_week,
)
st.plotly_chart(weekly_fig, use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

if daily is not None and not daily.empty:
    daily = filter_range(daily, "daily", filters["range"])
    daily["fecha"] = pd.to_datetime(daily["fecha"], errors="coerce")
    mask_hist_day = period_mask(daily, "daily")
    pick_col_day = f"picking_movs_esperados_{scenario}"
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="app-card">', unsafe_allow_html=True)
        card_title("Serie diaria de preparacion")
        st.plotly_chart(
            line_hist_forecast(daily, "fecha", [pick_col_day], ["Picking diario"], [PICKING_COLOR], mask_hist_day),
            use_container_width=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)
    with col2:
        st.markdown('<div class="app-card">', unsafe_allow_html=True)
        card_title("Comparacion semanal agregada")
        preview = weekly.tail(12).copy()
        preview["week_start_date"] = preview["week_start_date"].dt.strftime("%d/%m/%Y")
        st.plotly_chart(bar_chart(preview, x="week_start_date", y=pick_col_week, color=PICKING_COLOR), use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

visible = weekly[["week_start_date", "picking_movs_esperados_semana_p50", "picking_movs_esperados_semana_p80"] + ([ "picking_movs_reales_semana"] if "picking_movs_reales_semana" in weekly.columns else [])].copy()
st.dataframe(visible, use_container_width=True, hide_index=True)
st.download_button("Descargar tabla picking", visible.to_csv(index=False).encode("utf-8"), file_name=f"picking_{freq}_{scenario}.csv", mime="text/csv")

st.markdown('<div class="app-card">', unsafe_allow_html=True)
card_title("Interpretacion")
st.markdown("- El picking esperado aplica solo a entregas; las recogidas no generan picking.")
st.markdown("- El picking se desplaza por lead time y se ajusta por urgencia.")
st.markdown("- Si cae en festivo o fin de semana, se mueve al habil anterior.")
st.markdown("</div>", unsafe_allow_html=True)
