from __future__ import annotations

import streamlit as st


def apply_global_styles(dark_mode: bool = False) -> None:
    bg = "#0f172a" if dark_mode else "#f3f5f9"
    panel = "#111827" if dark_mode else "#ffffff"
    text = "#e5eefc" if dark_mode else "#18212f"
    subtext = "#94a3b8" if dark_mode else "#6b7280"

    st.markdown(
        f"""
        <style>
        .stApp {{
            background: radial-gradient(circle at top left, rgba(47,111,237,0.10), transparent 24%),
                        radial-gradient(circle at top right, rgba(14,165,233,0.10), transparent 18%),
                        {bg};
        }}
        [data-testid="stSidebar"] {{
            background: linear-gradient(180deg, #111827 0%, #0b1220 100%);
            border-right: 1px solid rgba(255,255,255,0.08);
        }}
        [data-testid="stSidebar"] * {{
            color: #e5eefc;
        }}
        div[data-testid="stMetric"] {{
            background: {panel};
            border: 1px solid rgba(15, 23, 42, 0.08);
            padding: 14px 16px;
            border-radius: 20px;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.08);
        }}
        .app-card {{
            background: {panel};
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 24px;
            padding: 1.15rem 1.2rem;
            box-shadow: 0 14px 34px rgba(15, 23, 42, 0.08);
            margin-bottom: 1rem;
        }}
        .app-header {{
            background: linear-gradient(135deg, rgba(47,111,237,0.14), rgba(14,165,233,0.06));
            border-radius: 28px;
            padding: 1.4rem 1.5rem;
            border: 1px solid rgba(47,111,237,0.08);
            margin-bottom: 1rem;
        }}
        .app-eyebrow {{
            letter-spacing: .18em;
            text-transform: uppercase;
            font-size: 0.72rem;
            color: {subtext};
            font-weight: 700;
        }}
        .app-title {{
            font-size: 2rem;
            line-height: 1.15;
            font-weight: 800;
            color: {text};
            margin: 0.4rem 0;
        }}
        .app-subtitle {{
            font-size: 0.96rem;
            color: {subtext};
            margin-bottom: 0;
        }}
        .section-title {{
            font-size: 1.1rem;
            font-weight: 800;
            color: {text};
            margin-bottom: 0.65rem;
        }}
        .hint {{
            color: {subtext};
            font-size: 0.9rem;
        }}
        .block-label {{
            font-size: 0.8rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: .14em;
            color: {subtext};
        }}
        .stTabs [data-baseweb="tab-list"] {{
            gap: 0.35rem;
        }}
        .stTabs [data-baseweb="tab"] {{
            border-radius: 999px;
            padding-left: 1rem;
            padding-right: 1rem;
            background: rgba(255,255,255,0.72);
        }}
        .stDownloadButton > button, .stButton > button {{
            border-radius: 999px;
            border: 1px solid rgba(15, 23, 42, 0.08);
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )
