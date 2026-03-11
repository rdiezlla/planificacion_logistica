from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from streamlit_app.components.cards import render_kpi_cards
from streamlit_app.components.charts import IN_COLOR, OUT_COLOR, PICKING_COLOR, bar_chart, line_hist_forecast
from streamlit_app.components.filters import render_global_sidebar
from streamlit_app.components.layout import card_title, render_page_header, show_missing_file
from streamlit_app.components.loaders import load_forecast_bundle
from streamlit_app.utils.business_logic import current_vs_mean, current_vs_previous, filter_range, period_mask, top_weeks
from streamlit_app.utils.formatters import fmt_date, fmt_int, fmt_number

render_page_header(
    "Resumen",
    "Vista ejecutiva del forecast de transporte y picking con foco en las proximas semanas y en las variaciones mas relevantes.",
)
filters = render_global_sidebar("Filtros globales")
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
metrics = {
    "Eventos entrega": f"eventos_entrega{suffix}_{scenario}",
    "M3 OUT": f"m3_out{suffix}_{scenario}",
    "Pales OUT": f"pales_out{suffix}_{scenario}",
    "Cajas OUT": f"cajas_out{suffix}_{scenario}",
    "Eventos recogida": f"eventos_recogida{suffix}_{scenario}",
    "M3 IN": f"m3_in{suffix}_{scenario}",
    "Pales IN": f"pales_in{suffix}_{scenario}",
    "Cajas IN": f"cajas_in{suffix}_{scenario}",
    "Picking esperado": f"picking_movs_esperados{suffix}_{scenario}",
}

cards = []
for label, col in metrics.items():
    if col not in df.columns:
        continue
    current, delta_prev = current_vs_previous(df[col])
    _, delta_avg = current_vs_mean(df[col], 8 if freq == "weekly" else 56)
    delta_display = delta_prev * 100 if delta_prev is not None else None
    help_text = f"Vs media periodo: {delta_avg * 100:.1f}%" if delta_avg is not None else None
    cards.append(
        {
            "label": label,
            "value": fmt_int(current) if "Eventos" in label or "Pales" in label or "Cajas" in label or "Picking" in label else fmt_number(current, 1),
            "delta": delta_display,
            "help": help_text,
        }
    )
render_kpi_cards(cards, columns=3)

st.markdown('<div class="app-card">', unsafe_allow_html=True)
card_title("Historico + forecast", "OUT, IN y picking esperado con separacion visual entre historico y forecast.")
main_fig = line_hist_forecast(
    df,
    x=date_col,
    y_cols=[metrics["Eventos entrega"], metrics["Eventos recogida"], metrics["Picking esperado"]],
    names=["OUT", "IN", "Picking"],
    colors=[OUT_COLOR, IN_COLOR, PICKING_COLOR],
    hist_mask=mask_hist,
)
st.plotly_chart(main_fig, use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

future_df = df.loc[~mask_hist].copy()
if future_df.empty:
    future_df = df.tail(8 if freq == "weekly" else 14).copy()

col1, col2 = st.columns([1.4, 1.0])
with col1:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Proximas semanas" if freq == "weekly" else "Proximos dias")
    preview_cols = [date_col] + [metrics["Eventos entrega"], metrics["M3 OUT"], metrics["Eventos recogida"], metrics["Picking esperado"]]
    st.dataframe(future_df[preview_cols].head(12), use_container_width=True, hide_index=True)
    st.download_button(
        "Descargar resumen",
        data=future_df[preview_cols].to_csv(index=False).encode("utf-8"),
        file_name=f"resumen_{freq}_{scenario}.csv",
        mime="text/csv",
    )
    st.markdown("</div>", unsafe_allow_html=True)

with col2:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Semanas de mayor carga")
    ranked = top_weeks(df, metrics["M3 OUT"] if metrics["M3 OUT"] in df.columns else metrics["Eventos entrega"], 8)
    if not ranked.empty:
        chart_df = ranked[[date_col, metrics["M3 OUT"]]].copy()
        chart_df[date_col] = chart_df[date_col].dt.strftime("%d/%m/%Y")
        st.plotly_chart(bar_chart(chart_df, x=date_col, y=metrics["M3 OUT"], color=OUT_COLOR), use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="app-card">', unsafe_allow_html=True)
card_title("Insights automaticos")
max_out = df.loc[df[metrics["M3 OUT"]].idxmax()] if metrics["M3 OUT"] in df.columns else None
max_pick = df.loc[df[metrics["Picking esperado"]].idxmax()] if metrics["Picking esperado"] in df.columns else None
risk_df = df.loc[df[metrics["Picking esperado"]] >= df[metrics["Picking esperado"]].quantile(0.8)].copy()
insights = []
if max_out is not None:
    insights.append(f"Semana con mayor carga OUT: {fmt_date(max_out[date_col])} con {fmt_number(max_out[metrics['M3 OUT']], 1)} m3.")
if max_pick is not None:
    insights.append(f"Periodo con mayor picking esperado: {fmt_date(max_pick[date_col])} con {fmt_int(max_pick[metrics['Picking esperado']])} movimientos.")
if not risk_df.empty:
    insights.append(f"Semanas con riesgo alto por P80: {len(risk_df)} periodos en el rango visible.")
for insight in insights:
    st.markdown(f"- {insight}")
st.markdown("</div>", unsafe_allow_html=True)
