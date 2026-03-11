from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import streamlit as st

LOGGER = logging.getLogger(__name__)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).strip() for col in out.columns]
    return out


@st.cache_data(show_spinner=False)
def read_csv_cached(path_str: str, usecols: tuple[str, ...] | None = None) -> pd.DataFrame:
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(path)
    kwargs = {"low_memory": False}
    if usecols:
        kwargs["usecols"] = list(usecols)
    df = pd.read_csv(path, **kwargs)
    return normalize_columns(df)


@st.cache_data(show_spinner=False)
def read_csv_filtered_chunks(
    path_str: str,
    usecols: tuple[str, ...],
    filters: tuple[tuple[str, float], ...],
    limit: int = 50000,
    chunksize: int = 100000,
) -> pd.DataFrame:
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(path)
    frames: list[pd.DataFrame] = []
    total = 0
    for chunk in pd.read_csv(path, usecols=list(usecols), chunksize=chunksize, low_memory=False):
        chunk = normalize_columns(chunk)
        for col, threshold in filters:
            if col in chunk.columns:
                chunk = chunk.loc[pd.to_numeric(chunk[col], errors="coerce").fillna(0) >= float(threshold)]
        if chunk.empty:
            continue
        frames.append(chunk)
        total += len(chunk)
        if total >= limit:
            break
    if not frames:
        return pd.DataFrame(columns=list(usecols))
    return pd.concat(frames, ignore_index=True).head(limit)


def file_info(path: Path) -> dict[str, object]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "modified": pd.Timestamp(path.stat().st_mtime, unit="s") if path.exists() else pd.NaT,
        "size_mb": round(path.stat().st_size / (1024 * 1024), 2) if path.exists() else None,
    }


def ensure_path(text: str | Path) -> Path:
    return text if isinstance(text, Path) else Path(str(text))
