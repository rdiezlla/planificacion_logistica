from __future__ import annotations

from typing import Iterable

import streamlit as st

from streamlit_app.utils.formatters import fmt_delta, safe_text


def render_kpi_cards(cards: Iterable[dict[str, object]], columns: int = 4) -> None:
    cards = list(cards)
    if not cards:
        return
    grid = st.columns(columns)
    for idx, card in enumerate(cards):
        col = grid[idx % columns]
        with col:
            st.metric(
                label=str(card.get("label", "")),
                value=safe_text(card.get("value", "-")),
                delta=fmt_delta(card["delta"]) if card.get("delta") is not None else None,
                delta_color="normal",
                help=str(card.get("help")) if card.get("help") else None,
            )
