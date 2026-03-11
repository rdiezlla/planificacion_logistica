from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from streamlit_app.components.cards import render_kpi_cards
from streamlit_app.components.charts import OPT_COLOR, bar_chart, histogram, scatter_or_bar
from streamlit_app.components.layout import card_title, render_page_header, show_missing_file
from streamlit_app.components.loaders import load_basket_bundle, load_rules_filtered
from streamlit_app.utils.formatters import fmt_int, fmt_number, fmt_percent

render_page_header(
    "Optimizacion picking",
    "Visualizacion del basket analysis para layout, co-localizacion y penalizacion operativa por multi-propietario.",
    eyebrow="Picking optimization",
)
bundle = load_basket_bundle(st.session_state["base_outputs_basket_dir"])

oper_summary = bundle["data"].get("transactions_summary_oper")
order_summary = bundle["data"].get("transactions_summary_order")
pairs_oper = bundle["data"].get("top_pairs_oper")
pairs_order = bundle["data"].get("top_pairs_order")
clusters_oper = bundle["data"].get("sku_clusters_oper")
clusters_order = bundle["data"].get("sku_clusters_order")
owner_penalty = bundle["data"].get("order_owner_penalty")
sku_freq_oper = bundle["data"].get("sku_frequency_oper")
sku_freq_order = bundle["data"].get("sku_frequency_order")

if oper_summary is None or oper_summary.empty:
    show_missing_file("transactions_summary_oper", bundle["files"]["transactions_summary_oper"]["path"])
    st.stop()

tabs = st.tabs(
    [
        "Resumen basket",
        "Pares frecuentes",
        "Reglas de asociacion",
        "Clusters SKU",
        "Penalizacion multi-propietario",
        "Vecinos SKU",
    ]
)

with tabs[0]:
    pct_multi = owner_penalty["is_multi_propietario"].mean() if owner_penalty is not None and not owner_penalty.empty else None
    pct_lines_multi = (
        owner_penalty.loc[owner_penalty["is_multi_propietario"].eq(1), "n_lineas_pi_totales"].sum()
        / max(owner_penalty["n_lineas_pi_totales"].sum(), 1)
        if owner_penalty is not None and not owner_penalty.empty
        else None
    )
    cards = [
        {"label": "Transacciones oper", "value": fmt_int(len(oper_summary))},
        {"label": "SKUs oper", "value": fmt_int(len(sku_freq_oper) if sku_freq_oper is not None else 0)},
        {"label": "Tamano medio cesta", "value": fmt_number(pd.to_numeric(oper_summary["n_skus"], errors="coerce").mean(), 1)},
        {"label": "% pedidos multi-prop.", "value": fmt_percent(pct_multi, 1)},
        {"label": "% lineas afectadas", "value": fmt_percent(pct_lines_multi, 1)},
    ]
    render_kpi_cards(cards, columns=5)
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="app-card">', unsafe_allow_html=True)
        card_title("Histograma tamano cesta oper")
        st.plotly_chart(histogram(oper_summary, "n_skus", OPT_COLOR), use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with col2:
        st.markdown('<div class="app-card">', unsafe_allow_html=True)
        card_title("Histograma tamano cesta order")
        if order_summary is not None and not order_summary.empty:
            st.plotly_chart(histogram(order_summary, "n_skus", "#0f766e"), use_container_width=True)
        else:
            show_missing_file("transactions_summary_order", bundle["files"]["transactions_summary_order"]["path"])
        st.markdown("</div>", unsafe_allow_html=True)
    if bundle.get("plots"):
        st.markdown('<div class="app-card">', unsafe_allow_html=True)
        card_title("Plots generados por el pipeline basket")
        img_cols = st.columns(2)
        for idx, img_path in enumerate(bundle["plots"][:4]):
            with img_cols[idx % 2]:
                st.image(str(img_path), caption=img_path.name, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

with tabs[1]:
    level = st.radio("Nivel", ["oper", "order"], horizontal=True)
    source = pairs_oper if level == "oper" else pairs_order
    if source is None or source.empty:
        show_missing_file(f"top_pairs_{level}", bundle["files"][f"top_pairs_{level}"]["path"])
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            min_support = st.number_input("Min support", min_value=0.0, max_value=1.0, value=0.01, step=0.005)
        with c2:
            min_lift = st.number_input("Min lift", min_value=0.0, value=1.2, step=0.1)
        with c3:
            search = st.text_input("Buscar SKU / descripcion", value="")
        pairs = source.copy()
        name_a = pairs["sku_name_a"].astype(str) if "sku_name_a" in pairs.columns else ""
        name_b = pairs["sku_name_b"].astype(str) if "sku_name_b" in pairs.columns else ""
        pairs["search_blob"] = (
            pairs["sku_a"].astype(str) + " " + pairs["sku_b"].astype(str) + " " + name_a + " " + name_b
        )
        pairs = pairs.loc[(pd.to_numeric(pairs["support_pair"], errors="coerce") >= min_support) & (pd.to_numeric(pairs["lift"], errors="coerce") >= min_lift)]
        if search:
            pairs = pairs.loc[pairs["search_blob"].str.contains(search, case=False, na=False)]
        pairs = pairs.sort_values(["count_pair", "lift"], ascending=[False, False]).head(500)
        st.dataframe(pairs.drop(columns=["search_blob"]), use_container_width=True, hide_index=True)
        bar_df = pairs.head(20).copy()
        bar_df["pair"] = bar_df["sku_a"].astype(str) + " + " + bar_df["sku_b"].astype(str)
        st.plotly_chart(bar_chart(bar_df, x="pair", y="count_pair", color=OPT_COLOR, orientation="h"), use_container_width=True)
        st.download_button(f"Descargar pares {level}", pairs.drop(columns=["search_blob"]).to_csv(index=False).encode("utf-8"), file_name=f"pairs_{level}.csv", mime="text/csv")
        st.caption("Estos pares son candidatos directos a co-localizacion si ademas comparten rotacion y restriccion fisica.")

with tabs[2]:
    level = st.radio("Reglas nivel", ["oper", "order"], horizontal=True, key="rules_level")
    c1, c2, c3 = st.columns(3)
    with c1:
        min_support = st.number_input("Support minimo", min_value=0.0, max_value=1.0, value=0.01, step=0.005, key="rule_support")
    with c2:
        min_conf = st.number_input("Confidence minima", min_value=0.0, max_value=1.0, value=0.2, step=0.05, key="rule_conf")
    with c3:
        min_lift = st.number_input("Lift minimo", min_value=0.0, value=1.2, step=0.1, key="rule_lift")
    rules_df, info = load_rules_filtered(st.session_state["base_outputs_basket_dir"], level, min_support, min_conf, min_lift, limit=30000)
    if rules_df is None or rules_df.empty:
        show_missing_file(f"rules_{level}", info["path"])
    else:
        st.dataframe(rules_df.sort_values(["lift", "confidence"], ascending=[False, False]), use_container_width=True, hide_index=True)
        st.download_button(f"Descargar reglas {level}", rules_df.to_csv(index=False).encode("utf-8"), file_name=f"rules_{level}_filtered.csv", mime="text/csv")

with tabs[3]:
    level = st.radio("Clusters nivel", ["oper", "order"], horizontal=True, key="cluster_level")
    clusters = clusters_oper if level == "oper" else clusters_order
    if clusters is None or clusters.empty:
        show_missing_file(f"sku_clusters_{level}", bundle["files"][f"sku_clusters_{level}"]["path"])
    else:
        cluster_stats = (
            clusters.groupby("cluster_id", dropna=False)
            .agg(cluster_size=("sku", "size"), cluster_transaction_count=("transaction_count", "sum"))
            .reset_index()
            .sort_values("cluster_transaction_count", ascending=False)
        )
        cards = [
            {"label": "N clusters", "value": fmt_int(cluster_stats["cluster_id"].nunique())},
            {"label": "Tamano medio cluster", "value": fmt_number(cluster_stats["cluster_size"].mean(), 1)},
        ]
        render_kpi_cards(cards, columns=2)
        selected_cluster = st.selectbox("Cluster", cluster_stats["cluster_id"].tolist())
        preview = cluster_stats.head(30).copy()
        preview["cluster_id"] = preview["cluster_id"].astype(str)
        st.plotly_chart(bar_chart(preview, x="cluster_id", y="cluster_transaction_count", color=OPT_COLOR), use_container_width=True)
        st.dataframe(clusters.loc[clusters["cluster_id"].eq(selected_cluster)], use_container_width=True, hide_index=True)

with tabs[4]:
    if owner_penalty is None or owner_penalty.empty:
        show_missing_file("order_owner_penalty", bundle["files"]["order_owner_penalty"]["path"])
    else:
        cards = [
            {"label": "% pedidos >1 propietario", "value": fmt_percent(owner_penalty["is_multi_propietario"].mean(), 1)},
            {"label": "% lineas PI afectadas", "value": fmt_percent(owner_penalty.loc[owner_penalty["is_multi_propietario"].eq(1), "n_lineas_pi_totales"].sum() / max(owner_penalty["n_lineas_pi_totales"].sum(), 1), 1)},
            {"label": "Max propietarios en pedido", "value": fmt_int(owner_penalty["n_propietarios_distintos"].max())},
        ]
        render_kpi_cards(cards, columns=3)
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(histogram(owner_penalty, "n_propietarios_distintos", OPT_COLOR, nbins=10), use_container_width=True)
        with col2:
            top_orders = owner_penalty.sort_values(["n_propietarios_distintos", "n_lineas_pi_totales"], ascending=[False, False]).head(20)
            st.plotly_chart(bar_chart(top_orders, x="pedido_id", y="n_lineas_pi_totales", color=OPT_COLOR, orientation="h"), use_container_width=True)
        st.markdown("- Aunque el pedido sea uno, la PDA obliga a recorridos separados por propietario.")
        st.markdown("- Por eso el layout debe optimizarse con vision `oper`, no solo `order`.")

with tabs[5]:
    neighbors = bundle["data"].get("sku_neighbors")
    if neighbors is None or neighbors.empty:
        show_missing_file("sku_neighbors", bundle["files"]["sku_neighbors"]["path"])
    else:
        sku = st.text_input("Buscar SKU")
        if sku:
            view = neighbors.loc[neighbors.astype(str).apply(lambda col: col.str.contains(sku, case=False, na=False)).any(axis=1)]
        else:
            view = neighbors.head(100)
        st.dataframe(view, use_container_width=True, hide_index=True)

st.markdown('<div class="app-card">', unsafe_allow_html=True)
card_title("Recomendaciones operativas")
if pairs_oper is not None and not pairs_oper.empty:
    top_pairs = pairs_oper.sort_values(["count_pair", "lift"], ascending=[False, False]).head(10)
    for _, rec in top_pairs.iterrows():
        st.markdown(f"- Co-localizar `{rec['sku_a']}` y `{rec['sku_b']}` en propietario/segmento `{rec['segment']}`.")
if clusters_oper is not None and not clusters_oper.empty:
    top_clusters = (
        clusters_oper.groupby(["segment", "cluster_id"], dropna=False)["transaction_count"]
        .sum()
        .reset_index()
        .sort_values("transaction_count", ascending=False)
        .head(5)
    )
    for _, rec in top_clusters.iterrows():
        st.markdown(f"- El cluster `{rec['cluster_id']}` del segmento `{rec['segment']}` deberia compartir zona de picking.")
if pairs_oper is not None and pairs_order is not None and not pairs_oper.empty and not pairs_order.empty:
    oper_set = set(zip(pairs_oper["sku_a"].astype(str), pairs_oper["sku_b"].astype(str)))
    order_set = set(zip(pairs_order["sku_a"].astype(str), pairs_order["sku_b"].astype(str)))
    diff = len(order_set - oper_set)
    st.markdown(f"- Hay al menos `{diff}` pares presentes en `order` que no aparecen igual en `oper`; ignorar propietario sesga decisiones de layout.")
st.markdown("</div>", unsafe_allow_html=True)
