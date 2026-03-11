from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


OUT_COLOR = "#5d86ff"
IN_COLOR = "#f6a74b"
PICKING_COLOR = "#7b61ff"
QUALITY_COLOR = "#64748b"
OPT_COLOR = "#32c48d"
ABC_COLOR = "#059669"


def _apply_base_layout(fig: go.Figure) -> go.Figure:
    fig.update_layout(
        margin=dict(l=12, r=12, t=8, b=8),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        paper_bgcolor="rgba(255,255,255,0)",
        plot_bgcolor="rgba(255,255,255,0)",
        hovermode="x unified",
        transition=dict(duration=550, easing="cubic-in-out"),
        uirevision="dashboard",
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor="rgba(148,163,184,0.18)", zeroline=False)
    return fig


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
                    line=dict(color=colors[idx], width=2.6, shape="spline", smoothing=0.6),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=df.loc[~hist_mask, x],
                    y=series.loc[~hist_mask],
                    mode="lines",
                    name=f"{names[idx]} forecast",
                    line=dict(color=colors[idx], width=3.1, dash="dash", shape="spline", smoothing=0.6),
                )
            )
        else:
            fig.add_trace(
                go.Scatter(
                    x=df[x],
                    y=series,
                    mode="lines",
                    name=names[idx],
                    line=dict(color=colors[idx], width=3.1, shape="spline", smoothing=0.6),
                )
            )
    return _apply_base_layout(fig)


def bar_chart(df: pd.DataFrame, x: str, y: str, color: str, orientation: str = "v", text: str | None = None) -> go.Figure:
    fig = px.bar(
        df,
        x=x if orientation == "v" else y,
        y=y if orientation == "v" else x,
        text=text,
        orientation=orientation,
    )
    fig.update_traces(
        marker_color=color,
        marker_line_width=0,
        opacity=0.92,
        hovertemplate="%{x}<br>%{y}<extra></extra>" if orientation == "v" else "%{y}<br>%{x}<extra></extra>",
    )
    return _apply_base_layout(fig)


def histogram(df: pd.DataFrame, column: str, color: str, nbins: int = 30) -> go.Figure:
    fig = px.histogram(df, x=column, nbins=nbins)
    fig.update_traces(marker_color=color, opacity=0.92)
    return _apply_base_layout(fig)


def scatter_or_bar(df: pd.DataFrame, x: str, y: str, color: str) -> go.Figure:
    fig = px.scatter(df, x=x, y=y, color_discrete_sequence=[color])
    fig.update_traces(marker=dict(size=9, opacity=0.78, line=dict(width=0)))
    return _apply_base_layout(fig)


def pareto_chart(df: pd.DataFrame, *, sku_col: str = "sku", value_col: str = "pick_lines", cumulative_col: str = "cumulative_pct", class_col: str = "abc_class") -> go.Figure:
    view = df.copy()
    if class_col not in view.columns:
        view[class_col] = "A"
    color_map = {"A": "#0f766e", "B": "#f6a74b", "C": "#94a3b8"}
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=view[sku_col].astype(str),
            y=pd.to_numeric(view[value_col], errors="coerce"),
            marker=dict(color=view[class_col].map(color_map).fillna(ABC_COLOR)),
            opacity=0.95,
            name="pick_lines",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=view[sku_col].astype(str),
            y=pd.to_numeric(view[cumulative_col], errors="coerce") * 100.0,
            mode="lines+markers",
            line=dict(color="#f05ba9", width=2.7, shape="spline", smoothing=0.55),
            marker=dict(size=6, color="#f05ba9"),
            name="cumulative %",
            yaxis="y2",
        )
    )
    fig.update_layout(
        yaxis=dict(title="pick_lines", gridcolor="rgba(148,163,184,0.18)"),
        yaxis2=dict(title="Cumulative %", overlaying="y", side="right", range=[0, 100]),
    )
    fig.add_hline(y=80, yref="y2", line_dash="dot", line_color="#32c48d", opacity=0.8)
    fig.add_hline(y=95, yref="y2", line_dash="dot", line_color="#f6a74b", opacity=0.8)
    fig.update_xaxes(tickangle=-40)
    return _apply_base_layout(fig)
