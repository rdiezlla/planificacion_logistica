from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from streamlit_app.components.cards import render_kpi_cards
from streamlit_app.components.charts import QUALITY_COLOR, bar_chart
from streamlit_app.components.filters import render_global_sidebar
from streamlit_app.components.layout import card_title, render_page_header, show_missing_file
from streamlit_app.components.loaders import load_forecast_bundle
from streamlit_app.utils.business_logic import infer_last_run_timestamp
from streamlit_app.utils.formatters import fmt_date, fmt_int, fmt_percent

render_page_header(
    "Calidad del modelo",
    "Revision de backtest, modelos registrados y cobertura del forecast para entender donde confiar mas en el modelo.",
    eyebrow="Model quality",
)
render_global_sidebar("Calidad", show_scenario=False, show_range=False)
bundle = load_forecast_bundle(st.session_state["base_outputs_dir"])

backtest = bundle["data"].get("backtest_metrics")
registry = bundle["data"].get("model_registry")
join_kpis = bundle["data"].get("join_kpis")
if backtest is None or backtest.empty:
    show_missing_file("backtest_metrics", bundle["files"]["backtest_metrics"]["path"])
    st.stop()

axis_opts = sorted(backtest["axis"].dropna().astype(str).unique().tolist())
target_opts = sorted(backtest["target"].dropna().astype(str).unique().tolist())
freq_opts = sorted(backtest["freq"].dropna().astype(str).unique().tolist())

colf1, colf2, colf3 = st.columns(3)
with colf1:
    axis_sel = st.selectbox("Axis", ["Todos"] + axis_opts)
with colf2:
    target_sel = st.selectbox("Target", ["Todos"] + target_opts)
with colf3:
    freq_sel = st.selectbox("Frecuencia", ["Todos"] + freq_opts)

filtered = backtest.copy()
if axis_sel != "Todos":
    filtered = filtered.loc[filtered["axis"].eq(axis_sel)]
if target_sel != "Todos":
    filtered = filtered.loc[filtered["target"].eq(target_sel)]
if freq_sel != "Todos":
    filtered = filtered.loc[filtered["freq"].eq(freq_sel)]

best_wape = pd.to_numeric(filtered.loc[filtered["model"].eq("model_p50"), "wape"], errors="coerce").min()
picking_cov = pd.to_numeric(
    backtest.loc[
        backtest["axis"].eq("workload_expected_from_service")
        & backtest["target"].astype(str).str.contains("picking", na=False),
        "coverage_empirical",
    ],
    errors="coerce",
).mean()
last_run = infer_last_run_timestamp(list(bundle["files"].values()))
cards = [
    {"label": "Mejor WAPE model_p50", "value": fmt_percent(best_wape, 1)},
    {"label": "Cobertura P80 picking", "value": fmt_percent(picking_cov, 1)},
    {"label": "N modelos registrados", "value": fmt_int(len(registry) if registry is not None else 0)},
    {"label": "Ultimo run inferido", "value": fmt_date(last_run)},
]
render_kpi_cards(cards, columns=4)

col1, col2 = st.columns([1.2, 1.0])
with col1:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("WAPE por target")
    plot_df = (
        filtered.loc[filtered["model"].eq("model_p50")]
        .groupby("target", dropna=False)["wape"]
        .mean()
        .reset_index()
        .sort_values("wape", ascending=False)
    )
    if not plot_df.empty:
        st.plotly_chart(bar_chart(plot_df, x="target", y="wape", color=QUALITY_COLOR), use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

with col2:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Naive vs modelo")
    compare_df = (
        filtered.loc[filtered["model"].isin(["naive_t7", "ma7", "ma28", "model_p50"])]
        .groupby("model", dropna=False)["wape"]
        .mean()
        .reset_index()
        .sort_values("wape", ascending=False)
    )
    if not compare_df.empty:
        st.plotly_chart(bar_chart(compare_df, x="model", y="wape", color="#334155"), use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

if registry is not None and not registry.empty:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Model registry")
    st.dataframe(registry, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="app-card">', unsafe_allow_html=True)
card_title("Backtest metrics")
st.dataframe(filtered, use_container_width=True, hide_index=True)
st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="app-card">', unsafe_allow_html=True)
card_title("Como interpretar")
st.markdown("- WAPE mide el error absoluto ponderado por el volumen real; cuanto mas bajo, mejor.")
st.markdown("- P50 es el escenario central. P80 representa un alto realista y sirve para tensionar capacidad.")
st.markdown("- En general, la lectura semanal suele ser mas estable y mas fiable que la diaria cuando hay mucho ruido operativo.")
if join_kpis is not None and not join_kpis.empty:
    st.markdown(f"- Coverage del join movimientos-servicio: **{fmt_percent(join_kpis['coverage'].iloc[0], 1)}**.")
st.markdown("</div>", unsafe_allow_html=True)
