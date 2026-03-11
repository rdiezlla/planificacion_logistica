from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


OUT_COLOR = "#2f6fed"
IN_COLOR = "#f59e0b"
PICKING_COLOR = "#4338ca"
QUALITY_COLOR = "#64748b"
OPT_COLOR = "#14b8a6"


def line_hist_forecast(df: pd.DataFrame, x: str, y_cols: list[str], names: list[str], colors: list[str], hist_mask: pd.Series | None = None) -> go.Figure:
    fig = go.Figure()
    for idx, col in enumerate(y_cols):
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        if hist_mask is not None:
            fig.add_trace(
                go.Scatter(
                    x=df.loc[hist_mask, x],
                    y=series.loc[hist_mask],
                    mode="lines",
                    name=f"{names[idx]} historico",
                    line=dict(color=colors[idx], width=2.5),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=df.loc[~hist_mask, x],
                    y=series.loc[~hist_mask],
                    mode="lines",
                    name=f"{names[idx]} forecast",
                    line=dict(color=colors[idx], width=3, dash="dash"),
                )
            )
        else:
            fig.add_trace(
                go.Scatter(x=df[x], y=series, mode="lines", name=names[idx], line=dict(color=colors[idx], width=3))
            )
    fig.update_layout(
        margin=dict(l=12, r=12, t=8, b=8),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(148,163,184,0.2)")
    return fig


def bar_chart(df: pd.DataFrame, x: str, y: str, color: str, orientation: str = "v", text: str | None = None) -> go.Figure:
    fig = px.bar(df, x=x if orientation == "v" else y, y=y if orientation == "v" else x, text=text, orientation=orientation)
    fig.update_traces(marker_color=color, marker_line_width=0)
    fig.update_layout(
        margin=dict(l=12, r=12, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(148,163,184,0.2)")
    return fig


def histogram(df: pd.DataFrame, column: str, color: str, nbins: int = 30) -> go.Figure:
    fig = px.histogram(df, x=column, nbins=nbins)
    fig.update_traces(marker_color=color)
    fig.update_layout(
        margin=dict(l=12, r=12, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def scatter_or_bar(df: pd.DataFrame, x: str, y: str, color: str) -> go.Figure:
    fig = px.scatter(df, x=x, y=y, color_discrete_sequence=[color])
    fig.update_traces(marker=dict(size=9, opacity=0.75))
    fig.update_layout(
        margin=dict(l=12, r=12, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig
