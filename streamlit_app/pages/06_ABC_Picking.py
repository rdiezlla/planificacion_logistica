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
from streamlit_app.utils.formatters import fmt_int, fmt_percent, safe_text


ABC_COLOR_MAP = {"A": "#0f766e", "B": "#f59e0b", "C": "#94a3b8"}
ABC_XYZ_COLOR_MAP = {
    "AX": "#0f766e",
    "AY": "#14b8a6",
    "AZ": "#0ea5e9",
    "BX": "#f59e0b",
    "BY": "#f97316",
    "BZ": "#ef4444",
    "CX": "#94a3b8",
    "CY": "#64748b",
    "CZ": "#334155",
    "ALOW_HISTORY": "#4338ca",
    "AUNKNOWN": "#475569",
    "BLOW_HISTORY": "#7c3aed",
    "BUNKNOWN": "#6b7280",
    "CLOW_HISTORY": "#6366f1",
    "CUNKNOWN": "#64748b",
}


def _safe_col(df: pd.DataFrame, col: str, default: object) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series([default] * len(df), index=df.index)


render_page_header(
    "ABC Picking",
    "Analisis Pareto y ABC-XYZ para slotting, layout y priorizacion de SKU segun rotacion operativa y estabilidad semanal.",
    eyebrow="ABC analysis",
)
render_global_sidebar("ABC Picking", show_scenario=False, show_frequency=False, show_range=False)
bundle = load_abc_bundle(st.session_state["base_outputs_abc_dir"])

annual_df = bundle["data"].get("abc_picking_annual")
quarterly_df = bundle["data"].get("abc_picking_quarterly")
ytd_df = bundle["data"].get("abc_picking_ytd")
summary_df = bundle["data"].get("abc_summary_by_period")
xyz_summary_df = bundle["data"].get("abc_xyz_summary_by_period")
owner_summary_df = bundle["data"].get("abc_owner_summary")
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
    mode = st.selectbox("Modo", ["ABC", "ABC-XYZ"])
    source = available.get(view_mode)

    owner_options = (
        source["owner_scope"].fillna("GLOBAL").astype(str).drop_duplicates().tolist()
        if source is not None and not source.empty and "owner_scope" in source.columns
        else ["GLOBAL"]
    )
    owner_options = sorted(owner_options, key=lambda v: (v != "GLOBAL", v))
    selected_owner = st.selectbox("Owner scope", owner_options, index=0)

    year_options = sorted(pd.to_numeric(source["year"], errors="coerce").dropna().astype(int).unique().tolist()) if source is not None and not source.empty else []
    selected_year = st.selectbox("Ano", year_options, index=len(year_options) - 1 if year_options else None) if year_options else None

    quarter_options: list[str] = []
    if view_mode == "quarterly" and source is not None and not source.empty and selected_year is not None:
        quarter_options = (
            source.loc[
                pd.to_numeric(source["year"], errors="coerce").eq(selected_year)
                & _safe_col(source, "owner_scope", "GLOBAL").astype(str).eq(selected_owner),
                "quarter",
            ]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        quarter_options = sorted(quarter_options)
    selected_quarter = st.selectbox("Trimestre", quarter_options, index=len(quarter_options) - 1 if quarter_options else None) if quarter_options else None

    top_n = int(st.slider("Top N", min_value=10, max_value=150, value=40, step=5))
    class_filter = st.multiselect("Clase ABC", ["A", "B", "C"], default=["A", "B", "C"])

    xyz_options_all = (
        source["xyz_class"].fillna("UNKNOWN").astype(str).drop_duplicates().tolist()
        if source is not None and not source.empty and "xyz_class" in source.columns
        else ["X", "Y", "Z", "LOW_HISTORY", "UNKNOWN"]
    )
    xyz_options = [val for val in ["X", "Y", "Z", "LOW_HISTORY", "UNKNOWN"] if val in xyz_options_all] + [val for val in xyz_options_all if val not in {"X", "Y", "Z", "LOW_HISTORY", "UNKNOWN"}]
    xyz_filter = st.multiselect("Clase XYZ", xyz_options, default=[val for val in xyz_options if val in {"X", "Y", "Z"}] or xyz_options)

    abc_xyz_options_all = (
        source["abc_xyz_class"].fillna("").astype(str).replace("", pd.NA).dropna().drop_duplicates().tolist()
        if source is not None and not source.empty and "abc_xyz_class" in source.columns
        else []
    )
    abc_xyz_options = sorted(abc_xyz_options_all)
    abc_xyz_filter = st.multiselect("Clase ABC-XYZ", abc_xyz_options, default=abc_xyz_options[:9] if abc_xyz_options else [])

    search = st.text_input("Buscar SKU / denominacion", value="")

if source is None or source.empty:
    show_missing_file(f"abc_{view_mode}", bundle["files"][f"abc_picking_{view_mode}"]["path"])
    st.stop()

selected = source.copy()
selected["owner_scope"] = _safe_col(selected, "owner_scope", "GLOBAL").fillna("GLOBAL").astype(str)
selected = selected.loc[selected["owner_scope"].eq(selected_owner)]
if selected_year is not None and "year" in selected.columns:
    selected = selected.loc[pd.to_numeric(selected["year"], errors="coerce").eq(int(selected_year))]
if view_mode == "quarterly" and selected_quarter:
    selected = selected.loc[selected["quarter"].astype(str).eq(selected_quarter)]
if class_filter:
    selected = selected.loc[_safe_col(selected, "abc_class", "").astype(str).isin(class_filter)]
if mode == "ABC-XYZ" and xyz_filter:
    selected = selected.loc[_safe_col(selected, "xyz_class", "UNKNOWN").astype(str).isin(xyz_filter)]
if mode == "ABC-XYZ" and abc_xyz_filter:
    selected = selected.loc[_safe_col(selected, "abc_xyz_class", "").astype(str).isin(abc_xyz_filter)]
if search:
    blob = selected["sku"].astype(str) + " " + selected["denominacion"].astype(str)
    selected = selected.loc[blob.str.contains(search, case=False, na=False)]
selected = selected.sort_values(["rank_in_period", "pick_lines"], ascending=[True, False]).reset_index(drop=True)

if selected.empty:
    st.info("No hay filas para los filtros seleccionados.")
    st.stop()

selected_period = str(selected["period_label"].iloc[0])
summary_filtered = (
    summary_df.loc[
        _safe_col(summary_df, "owner_scope", "GLOBAL").astype(str).eq(selected_owner)
        & summary_df["period_label"].astype(str).eq(selected_period)
    ].head(1)
    if summary_df is not None and not summary_df.empty
    else pd.DataFrame()
)
xyz_summary_filtered = (
    xyz_summary_df.loc[
        _safe_col(xyz_summary_df, "owner_scope", "GLOBAL").astype(str).eq(selected_owner)
        & xyz_summary_df["period_label"].astype(str).eq(selected_period)
        & xyz_summary_df["period_type"].astype(str).eq(view_mode)
    ].head(1)
    if xyz_summary_df is not None and not xyz_summary_df.empty
    else pd.DataFrame()
)
selected_changes = (
    changes_df.loc[
        _safe_col(changes_df, "owner_scope", "GLOBAL").astype(str).eq(selected_owner)
        & changes_df["curr_period"].astype(str).eq(selected_period)
        & changes_df["period_type"].astype(str).eq(view_mode)
    ].copy()
    if changes_df is not None and not changes_df.empty
    else pd.DataFrame()
)
if not selected_changes.empty:
    selected_changes["prev_score"] = selected_changes["prev_abc_class"].map({"C": 1, "B": 2, "A": 3}).fillna(0)
    selected_changes["curr_score"] = selected_changes["curr_abc_class"].map({"C": 1, "B": 2, "A": 3}).fillna(0)
    selected_changes["moved_up"] = selected_changes["curr_score"] > selected_changes["prev_score"]

top_row = selected.head(1)
ax_count = int(_safe_col(selected, "abc_xyz_class", "").astype(str).eq("AX").sum())
z_count = int(_safe_col(selected, "xyz_class", "UNKNOWN").astype(str).eq("Z").sum())
pct_ax = None
if not xyz_summary_filtered.empty and "pct_pick_lines_AX" in xyz_summary_filtered.columns:
    pct_ax = xyz_summary_filtered.iloc[0]["pct_pick_lines_AX"]

top_owner_value = "-"
if selected_owner == "GLOBAL" and owner_summary_df is not None and not owner_summary_df.empty:
    owner_non_global = owner_summary_df.loc[owner_summary_df["owner_scope"].astype(str).ne("GLOBAL")].copy()
    if not owner_non_global.empty:
        top_owner_value = str(owner_non_global.sort_values("total_pick_lines", ascending=False).iloc[0]["owner_scope"])
else:
    top_owner_value = selected_owner

cards = [
    {
        "label": "SKUs A",
        "value": fmt_int(_safe_col(selected, "abc_class", "").astype(str).eq("A").sum()),
        "help": "Numero de SKUs clase A en el alcance seleccionado.",
    },
    {
        "label": "% pick_lines en A",
        "value": fmt_percent(None if summary_filtered.empty else summary_filtered.iloc[0]["pct_pick_lines_A"], 1),
        "help": "Concentracion operativa cubierta por clase A.",
    },
    {
        "label": "SKUs AX",
        "value": fmt_int(ax_count),
        "help": "SKUs de alta rotacion y estabilidad semanal.",
    },
    {
        "label": "% pick_lines AX",
        "value": fmt_percent(pct_ax, 1),
        "help": "Peso operativo de AX dentro del periodo/owner filtrado.",
    },
    {
        "label": "Top owner" if selected_owner == "GLOBAL" else "Owner scope",
        "value": safe_text(top_owner_value),
        "help": "En GLOBAL muestra el propietario con mayor carga; si no, el owner actualmente filtrado.",
    },
    {
        "label": "SKUs Z",
        "value": fmt_int(z_count),
        "help": "SKUs con alta volatilidad semanal en pick_lines.",
    },
]
render_kpi_cards(cards, columns=6)

top_view = selected.head(top_n).copy()
if layout_df is not None and not layout_df.empty:
    layout_filtered = layout_df.loc[_safe_col(layout_df, "owner_scope", "GLOBAL").astype(str).eq(selected_owner)].copy()
    top_view = top_view.merge(
        layout_filtered[["owner_scope", "sku", "recommendation_tag"]].drop_duplicates(subset=["owner_scope", "sku"]),
        on=["owner_scope", "sku"],
        how="left",
    )
else:
    layout_filtered = pd.DataFrame()
    top_view["recommendation_tag"] = pd.NA

upper1, upper2 = st.columns([1.7, 1.1])
with upper1:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Pareto del periodo", hint="El filtro por propietario recalcula el ranking real dentro de ese owner, no solo la visualizacion.")
    st.plotly_chart(pareto_chart(top_view), use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

with upper2:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    chart_title = "Distribucion ABC-XYZ" if mode == "ABC-XYZ" else "Distribucion ABC"
    card_title(chart_title, hint=f"Periodo seleccionado: {selected_period} | owner_scope: {selected_owner}")
    if mode == "ABC-XYZ":
        dist = (
            top_view.groupby("abc_xyz_class", dropna=False)["pick_lines"].sum().reset_index()
            if "abc_xyz_class" in top_view.columns
            else pd.DataFrame(columns=["abc_xyz_class", "pick_lines"])
        )
        fig = px.pie(dist, names="abc_xyz_class", values="pick_lines", color="abc_xyz_class", color_discrete_map=ABC_XYZ_COLOR_MAP)
    else:
        dist = selected.groupby("abc_class", dropna=False)["pick_lines"].sum().reindex(["A", "B", "C"], fill_value=0).reset_index()
        dist.columns = ["abc_class", "pick_lines"]
        fig = px.pie(dist, names="abc_class", values="pick_lines", color="abc_class", color_discrete_map=ABC_COLOR_MAP)
    fig.update_layout(margin=dict(l=8, r=8, t=8, b=8), paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="app-card">', unsafe_allow_html=True)
card_title("ABC-XYZ scatter", hint="x = media semanal, y = coeficiente de variacion semanal. Sirve para separar rotacion, estabilidad y criticidad.")
scatter_df = top_view.copy()
scatter_df["plot_color"] = scatter_df["abc_xyz_class"] if mode == "ABC-XYZ" else scatter_df["abc_class"]
scatter_df["mean_weekly_pick_lines"] = pd.to_numeric(_safe_col(scatter_df, "mean_weekly_pick_lines", 0), errors="coerce")
scatter_df["cv_weekly"] = pd.to_numeric(_safe_col(scatter_df, "cv_weekly", 0), errors="coerce")
scatter_df["pick_lines"] = pd.to_numeric(_safe_col(scatter_df, "pick_lines", 0), errors="coerce")
fig_scatter = px.scatter(
    scatter_df,
    x="mean_weekly_pick_lines",
    y="cv_weekly",
    size="pick_lines",
    color="plot_color",
    hover_data=["sku", "denominacion", "owner_scope", "abc_class", "xyz_class", "abc_xyz_class"],
    color_discrete_map=ABC_XYZ_COLOR_MAP if mode == "ABC-XYZ" else ABC_COLOR_MAP,
)
fig_scatter.add_hline(y=0.5, line_dash="dash", line_color="#0f766e", opacity=0.7)
fig_scatter.add_hline(y=1.0, line_dash="dash", line_color="#f59e0b", opacity=0.7)
fig_scatter.update_layout(
    margin=dict(l=12, r=12, t=8, b=8),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
)
st.plotly_chart(fig_scatter, use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="app-card">', unsafe_allow_html=True)
card_title("Tabla enriquecida", hint="ABC sigue basandose en pick_lines. XYZ añade estabilidad semanal para priorizar mejor el slotting.")
table_columns = [
    "owner_scope",
    "sku",
    "denominacion",
    "pick_lines",
    "pick_qty",
    "n_orders",
    "cumulative_pct",
    "abc_class",
    "xyz_class",
    "abc_xyz_class",
    "mean_weekly_pick_lines",
    "cv_weekly",
    "n_weeks_observed",
    "rank_in_period",
    "recommendation_tag",
]
table_df = top_view[[col for col in table_columns if col in top_view.columns]].copy()
st.dataframe(table_df, use_container_width=True, hide_index=True)
st.download_button(
    "Descargar CSV filtrado",
    table_df.to_csv(index=False).encode("utf-8"),
    file_name=f"abc_picking_{selected_owner}_{selected_period}.csv",
    mime="text/csv",
)
st.markdown("</div>", unsafe_allow_html=True)

lower1, lower2 = st.columns([1.2, 1.2])
with lower1:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Cambios vs periodo anterior", hint="Detecta SKUs que suben o bajan de clase ABC entre periodos consecutivos dentro del mismo owner.")
    if selected_changes.empty:
        st.info("No hay comparativa disponible para este periodo/owner.")
    else:
        metric_col = "abc_xyz_change" if mode == "ABC-XYZ" and "abc_xyz_change" in selected_changes.columns else "class_change"
        change_counts = selected_changes[metric_col].value_counts().reset_index()
        change_counts.columns = [metric_col, "n_skus"]
        st.plotly_chart(bar_chart(change_counts.head(12), x=metric_col, y="n_skus", color=OPT_COLOR), use_container_width=True)
        view_changes_cols = [
            "sku",
            "prev_period",
            "curr_period",
            "prev_abc_class",
            "curr_abc_class",
            "prev_xyz_class",
            "curr_xyz_class",
            metric_col,
            "prev_rank",
            "curr_rank",
            "rank_delta",
        ]
        view_changes = selected_changes[[col for col in view_changes_cols if col in selected_changes.columns]].sort_values(["rank_delta"], ascending=False)
        st.dataframe(view_changes, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

with lower2:
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Vista para layout", hint="AX = premium claro; AZ = premium con flexibilidad; CZ = baja prioridad o revision de espacio.")
    if layout_filtered.empty:
        show_missing_file("abc_for_layout_candidates", bundle["files"]["abc_for_layout_candidates"]["path"])
    else:
        st.dataframe(layout_filtered.head(25), use_container_width=True, hide_index=True)
        rec_counts = layout_filtered["recommendation_tag"].value_counts().reset_index()
        rec_counts.columns = ["recommendation_tag", "n_skus"]
        st.plotly_chart(bar_chart(rec_counts, x="recommendation_tag", y="n_skus", color=ABC_COLOR), use_container_width=True)
    st.markdown("- `KEEP_FRONT_STABLE`: AX, maxima accesibilidad con demanda estable.")
    st.markdown("- `KEEP_FRONT_FLEX`: AZ, alta rotacion pero con picos; reservar flexibilidad.")
    st.markdown("- `ACCESSIBLE_STABLE`: BX, accesible sin consumir ubicacion premium.")
    st.markdown("- `MONITOR_VOLATILE`: BZ, revisar antes de consolidar layout.")
    st.markdown("- `REVIEW_SPACE`: CZ, cuestionar espacio prime o sobredimensionado.")
    st.markdown("</div>", unsafe_allow_html=True)

if bundle.get("plots"):
    st.markdown('<div class="app-card">', unsafe_allow_html=True)
    card_title("Diagnosticos generados por el pipeline", hint="Imagenes estaticas exportadas en `outputs_abc/plots`.")
    plot_cols = st.columns(2)
    for idx, img_path in enumerate(bundle["plots"][:8]):
        with plot_cols[idx % 2]:
            st.image(str(img_path), caption=img_path.name, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)
