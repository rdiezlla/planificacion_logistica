from __future__ import annotations

import streamlit as st


def apply_global_styles(dark_mode: bool = False) -> None:
    page_bg = "#dfe3e8" if not dark_mode else "#0f172a"
    shell_bg = "#fbfbfc" if not dark_mode else "#111827"
    sidebar_bg = "#f7f7f8" if not dark_mode else "#0b1220"
    panel = "#ffffff" if not dark_mode else "#0f172a"
    panel_alt = "#f7f8fa" if not dark_mode else "#111827"
    border = "rgba(15, 23, 42, 0.08)" if not dark_mode else "rgba(255,255,255,0.08)"
    text = "#171717" if not dark_mode else "#e5eefc"
    subtext = "#6b7280" if not dark_mode else "#94a3b8"
    muted = "#9ca3af" if not dark_mode else "#64748b"
    accent = "#f05ba9"
    accent_2 = "#55c38a"
    accent_3 = "#5d86ff"

    st.markdown(
        f"""
        <style>
        .stApp {{
            background:
                radial-gradient(circle at top left, rgba(240,91,169,0.08), transparent 24%),
                radial-gradient(circle at top right, rgba(93,134,255,0.08), transparent 20%),
                linear-gradient(180deg, {page_bg} 0%, {page_bg} 100%);
            color: {text};
            font-family: "Segoe UI", "Inter", "Helvetica Neue", sans-serif;
        }}

        .main .block-container {{
            max-width: 1480px;
            padding-top: 1.5rem;
            padding-bottom: 2.25rem;
            padding-left: 1.4rem;
            padding-right: 1.4rem;
        }}

        [data-testid="stAppViewContainer"] > .main {{
            background: transparent;
        }}

        [data-testid="stSidebar"] {{
            background: linear-gradient(180deg, {sidebar_bg} 0%, #f2f3f5 100%);
            border-right: 1px solid {border};
            box-shadow: inset -1px 0 0 rgba(255,255,255,0.35);
        }}

        [data-testid="stSidebar"] .stMarkdown,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] span {{
            color: {text};
        }}

        [data-testid="stSidebar"] [data-baseweb="select"] > div,
        [data-testid="stSidebar"] .stTextInput > div > div,
        [data-testid="stSidebar"] .stDateInput > div > div {{
            border-radius: 14px;
            border: 1px solid rgba(17, 24, 39, 0.06);
            background: rgba(255,255,255,0.86);
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.7);
        }}

        [data-testid="stSidebarNav"] {{
            background: rgba(255,255,255,0.66);
            border: 1px solid rgba(17, 24, 39, 0.05);
            border-radius: 20px;
            padding: 0.4rem;
            margin-top: 0.55rem;
        }}

        [data-testid="stSidebarNav"] a {{
            border-radius: 14px;
            margin-bottom: 0.2rem;
        }}

        [data-testid="stSidebarNav"] a:hover {{
            background: rgba(93,134,255,0.08);
        }}

        [data-testid="stSidebarNav"] a[aria-current="page"] {{
            background: #ffffff;
            box-shadow: 0 6px 18px rgba(15, 23, 42, 0.08);
            border: 1px solid rgba(17, 24, 39, 0.04);
        }}

        .app-header {{
            background: linear-gradient(180deg, rgba(255,255,255,0.82), rgba(255,255,255,0.76));
            backdrop-filter: blur(12px);
            border-radius: 26px;
            padding: 1rem 1.2rem 1.05rem 1.2rem;
            border: 1px solid rgba(17, 24, 39, 0.06);
            box-shadow: 0 16px 44px rgba(15, 23, 42, 0.08);
            margin-bottom: 1rem;
            position: relative;
            overflow: hidden;
        }}

        .app-header::after {{
            content: "";
            position: absolute;
            inset: 0;
            background:
                linear-gradient(90deg, transparent 0%, rgba(240,91,169,0.07) 42%, transparent 68%),
                linear-gradient(90deg, transparent 0%, rgba(93,134,255,0.05) 80%, transparent 100%);
            pointer-events: none;
        }}

        .app-eyebrow {{
            letter-spacing: .14em;
            text-transform: uppercase;
            font-size: 0.68rem;
            color: {muted};
            font-weight: 700;
            position: relative;
            z-index: 1;
        }}

        .app-title {{
            font-size: 1.9rem;
            line-height: 1.05;
            font-weight: 700;
            color: {text};
            margin: 0.35rem 0 0.25rem 0;
            position: relative;
            z-index: 1;
        }}

        .app-subtitle {{
            font-size: 0.94rem;
            color: {subtext};
            margin-bottom: 0;
            max-width: 980px;
            position: relative;
            z-index: 1;
        }}

        div[data-testid="stMetric"] {{
            background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(247,248,250,0.98));
            border: 1px solid rgba(17, 24, 39, 0.05);
            padding: 0.95rem 1rem;
            border-radius: 22px;
            box-shadow: 0 10px 26px rgba(15, 23, 42, 0.06);
            min-height: 116px;
        }}

        div[data-testid="stMetric"] label {{
            color: {subtext};
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: .08em;
            font-weight: 700;
        }}

        div[data-testid="stMetricValue"] {{
            color: {text};
            font-weight: 700;
            letter-spacing: -0.03em;
        }}

        div[data-testid="stMetricDelta"] {{
            color: {muted};
        }}

        .app-card {{
            background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(250,250,251,0.96));
            border: 1px solid rgba(17, 24, 39, 0.06);
            border-radius: 26px;
            padding: 1.15rem 1.2rem;
            box-shadow: 0 16px 40px rgba(15, 23, 42, 0.08);
            margin-bottom: 1rem;
            position: relative;
            overflow: hidden;
        }}

        .app-card::before {{
            content: "";
            position: absolute;
            inset: 0 auto auto 0;
            width: 100%;
            height: 1px;
            background: linear-gradient(90deg, rgba(240,91,169,0.35), rgba(93,134,255,0.18), transparent);
        }}

        .section-title {{
            font-size: 1rem;
            font-weight: 700;
            color: {text};
            margin-bottom: 0.45rem;
            letter-spacing: -0.01em;
        }}

        .hint {{
            color: {subtext};
            font-size: 0.88rem;
            margin-bottom: 0.25rem;
        }}

        .block-label {{
            font-size: 0.76rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: .14em;
            color: {muted};
        }}

        .stTabs [data-baseweb="tab-list"] {{
            gap: 0.35rem;
            background: rgba(255,255,255,0.58);
            padding: 0.35rem;
            border-radius: 999px;
            border: 1px solid rgba(17, 24, 39, 0.05);
            width: fit-content;
        }}

        .stTabs [data-baseweb="tab"] {{
            border-radius: 999px;
            padding-left: 1rem;
            padding-right: 1rem;
            color: {subtext};
            background: transparent;
            font-weight: 600;
        }}

        .stTabs [aria-selected="true"] {{
            background: #ffffff !important;
            color: {text} !important;
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
        }}

        .stDataFrame, [data-testid="stTable"] {{
            border-radius: 18px;
            overflow: hidden;
            border: 1px solid rgba(17, 24, 39, 0.05);
        }}

        .stDownloadButton > button,
        .stButton > button {{
            border-radius: 999px;
            border: 1px solid rgba(17, 24, 39, 0.06);
            background: linear-gradient(180deg, #ffffff, {panel_alt});
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
            color: {text};
            padding-left: 1rem;
            padding-right: 1rem;
        }}

        .stDownloadButton > button:hover,
        .stButton > button:hover {{
            border-color: rgba(93,134,255,0.18);
            color: {text};
        }}

        .stAlert {{
            border-radius: 18px;
            border: 1px solid rgba(17, 24, 39, 0.06);
        }}

        [data-testid="stPlotlyChart"] {{
            border-radius: 22px;
            overflow: hidden;
        }}

        .stMultiSelect [data-baseweb="tag"] {{
            background: rgba(93,134,255,0.08);
            border-radius: 999px;
            border: 1px solid rgba(93,134,255,0.08);
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )
