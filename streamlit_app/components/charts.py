from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


OUT_COLOR = "#2f6fed"
IN_COLOR = "#f59e0b"
PICKING_COLOR = "#4338ca"
QUALITY_COLOR = "#64748b"
OPT_COLOR = "#14b8a6"
ABC_COLOR = "#059669"


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


def pareto_chart(df: pd.DataFrame, *, sku_col: str = "sku", value_col: str = "pick_lines", cumulative_col: str = "cumulative_pct", class_col: str = "abc_class") -> go.Figure:
    view = df.copy()
    if class_col not in view.columns:
        view[class_col] = "A"
    color_map = {"A": "#0f766e", "B": "#f59e0b", "C": "#94a3b8"}
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=view[sku_col].astype(str),
            y=pd.to_numeric(view[value_col], errors="coerce"),
            marker=dict(color=view[class_col].map(color_map).fillna(ABC_COLOR)),
            name="pick_lines",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=view[sku_col].astype(str),
            y=pd.to_numeric(view[cumulative_col], errors="coerce") * 100.0,
            mode="lines+markers",
            line=dict(color="#1e293b", width=2.5),
            name="cumulative %",
            yaxis="y2",
        )
    )
    fig.update_layout(
        margin=dict(l=12, r=12, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis=dict(title="pick_lines", gridcolor="rgba(148,163,184,0.2)"),
        yaxis2=dict(title="Cumulative %", overlaying="y", side="right", range=[0, 100]),
        hovermode="x unified",
    )
    fig.add_hline(y=80, yref="y2", line_dash="dash", line_color="#0f766e", opacity=0.7)
    fig.add_hline(y=95, yref="y2", line_dash="dash", line_color="#f59e0b", opacity=0.7)
    fig.update_xaxes(showgrid=False, tickangle=-45)
    return fig
