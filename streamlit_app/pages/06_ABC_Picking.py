from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from streamlit_app.components.cards import render_kpi_cards
from streamlit_app.components.charts import ABC_COLOR, OPT_COLOR, bar_chart, pareto_chart
from streamlit_app.components.filters import render_global_sidebar
from streamlit_app.components.layout import card_title, render_page_header, show_missing_file
from streamlit_app.components.loaders import load_abc_bundle
from streamlit_app.utils.formatters import fmt_int, fmt_number, fmt_percent


render_page_header(
    "ABC Picking",
    "Analisis Pareto / clasificacion ABC para priorizar slotting, layout y decisiones de reubicacion por rotacion operativa.",
    eyebrow="ABC analysis",
)
render_global_sidebar("ABC Picking", show_scenario=False, show_frequency=False, show_range=False)
bundle = load_abc_bundle(st.session_state["base_outputs_abc_dir"])

annual_df = bundle["data"].get("abc_picking_annual")
quarterly_df = bundle["data"].get("abc_picking_quarterly")
ytd_df = bundle["data"].get("abc_picking_ytd")
summary_df = bundle["data"].get("abc_summary_by_period")
changes_df = bundle["data"].get("abc_top_changes")
layout_df = bundle["data"].get("abc_for_layout_candidates")

available = {
    "annual": annual_df,
    "quarterly": quarterly_df,
    "ytd": ytd_df,
}
if all(df is None or df.empty for df in available.values()):
    show_missing_file("outputs_abc", bundle["files"]["abc_picking_annual"]["path"])
    st.stop()

with st.sidebar:
    view_mode = st.selectbox(
        "Vista",
        ["annual", "quarterly", "ytd"],
        format_func=lambda v: {"annual": "Anual", "quarterly": "Trimestral", "ytd": "YTD"}[v],
    )
    source = available.get(view_mode)
    year_options = sorted(pd.to_numeric(source["year"], errors="coerce").dropna().astype(int).unique().tolist()) if source is not None and not source.empty else []
    selected_year = st.selectbox("Ano", year_options, index=len(year_options) - 1 if year_options else None) if year_options else None
    quarter_options = []
    if view_mode == "quarterly" and source is not None and not source.empty and selected_year is not None:
        quarter_options = (
            source.loc[pd.to_numeric(source["year"], errors="coerce").eq(selected_year), "quarter"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        quarter_options = sorted(quarter_options)
    selected_quarter = st.selectbox("Trimestre", quarter_options, index=len(quarter_options) - 1 if quarter_options else None) if quarter_options else None
    top_n = int(st.slider("Top N", min_value=10, max_value=100, value=30, step=5))
    class_filter = st.multiselect("Clase ABC", ["A", "B", "C"], default=["A", "B", "C"])
    search = st.text_input("Buscar SKU / denominacion", value="")

if source is None or source.empty:
    show_missing_file(f"abc_{view_mode}", bundle["files"][f"abc_picking_{view_mode}"]["path"])
    st.stop()

selected = source.copy()
if selected_year is not None and "year" in selected.columns:
    selected = selected.loc[pd.to_numeric(selected["year"], errors="coerce").eq(int(selected_year))]
if view_mode == "quarterly" and selected_quarter:
    selected = selected.loc[selected["quarter"].astype(str).eq(selected_quarter)]
if class_filter:
    selected = selected.loc[selected["abc_class"].astype(str).isin(class_filter)]
if search:
    blob = selected["sku"].astype(str) + " " + selected["denominacion"].astype(str)
    selected = selected.loc[blob.str.contains(search, case=False, na=False)]
selected = selected.sort_values(["rank_in_period", "pick_lines"], ascending=[True, False]).reset_index(drop=True)

if selected.empty:
    st.info("No hay filas para los filtros seleccionados.")
    st.stop()

selected_period = str(selected["period_label"].iloc[0])
selected_changes = (
    changes_df.loc[changes_df["curr_period"].astype(str).eq(selected_period)].copy()
    if changes_df is not None and not changes_df.empty
    else pd.DataFrame()
)
if not selected_changes.empty:
    selected_changes["prev_score"] = selected_changes["prev_abc_class"].map({"C": 1, "B": 2, "A": 3}).fillna(0)
    selected_changes["curr_score"] = selected_changes["curr_abc_class"].map({"C": 1, "B": 2, "A": 3}).fillna(0)
    selected_changes["moved_up"] = selected_changes["curr_score"] > selected_changes["prev_score"]

period_summary = (
    summary_df.loc[summary_df["period_label"].astype(str).eq(selected_period)].head(1)
    if summary_df is not None and not summary_df.empty
    else pd.DataFrame()
)
top_row = selected.head(1)
cards = [
    {
        "label": "SKUs A",
        "value": fmt_int(selected["abc_class"].eq("A").sum()),
        "help": "Numero de SKUs clasificados como A en el periodo filtrado.",
    },
    {
        "label": "% pick_lines en A",
        "value": fmt_percent(None if period_summary.empty else period_summary.iloc[0]["pct_pick_lines_A"], 1),
        "help": "Concentracion operativa cubierta por la clase A.",
    },
    {
        "label": "Top SKU",
        "value": "-" if top_row.empty else str(top_row.iloc[0]["sku"]),
        "help": "SKU con mayor numero de lineas de picking en el periodo.",
    },
    {
        "label": "SKUs que suben",
        "value": fmt_int(0 if selected_changes.empty else int(selected_changes["moved_up"].sum())),
        "help": "SKUs que mejoran de clase respecto al periodo anterior comparable.",
    },
]
render_kpi_cards(cards, columns=4)

top_view = selected.head(top_n).copy()
if layout_df is not None and not layout_df.empty:
    top_view = top_view.merge(
        layout_df[["sku", "recommendation_tag"]].drop_duplicates(subset=["sku"]),
        on="sku",
        how="left",
    )
else:
    top_view["recommendation_tag"] = pd.NA

col1, col2 = st.columns([1.8, 1.1])
with col1:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Pareto del periodo", hint="Barras por pick_lines y linea de acumulado. Las clases A/B/C se colorean para lectura rapida.")
    st.plotly_chart(pareto_chart(top_view), use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

with col2:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Distribucion ABC", hint=f"Periodo seleccionado: {selected_period}")
    abc_dist = (
        selected.groupby("abc_class", dropna=False)["pick_lines"]
        .sum()
        .reindex(["A", "B", "C"], fill_value=0)
        .reset_index()
    )
    abc_dist.columns = ["abc_class", "pick_lines"]
    fig = px.pie(abc_dist, names="abc_class", values="pick_lines", color="abc_class", color_discrete_map={"A": "#0f766e", "B": "#f59e0b", "C": "#94a3b8"})
    fig.update_layout(margin=dict(l=8, r=8, t=8, b=8), paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="app-card">', unsafe_allow_html=True)
card_title("Tabla ABC", hint="La clasificacion principal se basa en `pick_lines`. `pick_qty` y `n_orders` son metricas auxiliares.")
table_df = top_view[
    [
        "sku",
        "denominacion",
        "pick_lines",
        "pick_qty",
        "n_orders",
        "cumulative_pct",
        "abc_class",
        "rank_in_period",
        "recommendation_tag",
    ]
].copy()
st.dataframe(table_df, use_container_width=True, hide_index=True)
st.download_button(
    "Descargar CSV filtrado",
    table_df.to_csv(index=False).encode("utf-8"),
    file_name=f"abc_picking_{selected_period}.csv",
    mime="text/csv",
)
st.markdown("</div>", unsafe_allow_html=True)

lower1, lower2 = st.columns([1.2, 1.2])
with lower1:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Cambios vs periodo anterior", hint="Sirve para detectar subidas y bajadas de importancia relativa entre periodos consecutivos.")
    if selected_changes.empty:
        st.info("No hay comparativa disponible para este periodo.")
    else:
        change_counts = selected_changes["class_change"].value_counts().reset_index()
        change_counts.columns = ["class_change", "n_skus"]
        st.plotly_chart(bar_chart(change_counts.head(12), x="class_change", y="n_skus", color=OPT_COLOR), use_container_width=True)
        view_changes = selected_changes[
            ["sku", "prev_period", "curr_period", "prev_abc_class", "curr_abc_class", "class_change", "prev_rank", "curr_rank", "rank_delta"]
        ].sort_values(["class_change", "rank_delta"], ascending=[True, False])
        st.dataframe(view_changes, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

with lower2:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Vista para layout", hint="Enfoca las decisiones de slotting y reubicacion sobre el ultimo periodo disponible.")
    if layout_df is None or layout_df.empty:
        show_missing_file("abc_for_layout_candidates", bundle["files"]["abc_for_layout_candidates"]["path"])
    else:
        st.dataframe(layout_df.head(25), use_container_width=True, hide_index=True)
        rec_counts = layout_df["recommendation_tag"].value_counts().reset_index()
        rec_counts.columns = ["recommendation_tag", "n_skus"]
        st.plotly_chart(bar_chart(rec_counts, x="recommendation_tag", y="n_skus", color=ABC_COLOR), use_container_width=True)
    st.markdown("- `KEEP_FRONT`: SKU A estable, candidato a posiciones de maxima accesibilidad.")
    st.markdown("- `REVIEW_UPGRADE`: SKU que sube a A; revisar si merece re-slotting.")
    st.markdown("- `REVIEW_DOWNGRADE`: SKU que baja desde A; revisar si sigue ocupando ubicaciones premium.")
    st.markdown("- `LOW_PRIORITY`: SKU C estable; baja prioridad para zona caliente.")
    st.markdown("</div>", unsafe_allow_html=True)

if bundle.get("plots"):
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Diagnosticos generados por el pipeline", hint="Imagenes estáticas exportadas en `outputs_abc/plots`.")
    plot_cols = st.columns(2)
    for idx, img_path in enumerate(bundle["plots"][:6]):
        with plot_cols[idx % 2]:
            st.image(str(img_path), caption=img_path.name, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)
