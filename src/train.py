from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

LOGGER = logging.getLogger(__name__)

try:
    from lightgbm import LGBMRegressor

    HAS_LIGHTGBM = True
except Exception:  # pragma: no cover
    LGBMRegressor = None
    HAS_LIGHTGBM = False


@dataclass
class ModelArtifact:
    target: str
    axis: str
    freq: str
    path: Path


def build_feature_sets(df: pd.DataFrame, target_cols: list[str]) -> tuple[list[str], list[str], list[str]]:
    ignore = set(target_cols + ["date", "is_historical", "excluded_year"])
    feature_cols = [c for c in df.columns if c not in ignore]

    categorical = []
    numerical = []
    for c in feature_cols:
        dt = df[c].dtype
        if pd.api.types.is_object_dtype(dt) or pd.api.types.is_categorical_dtype(dt):
            categorical.append(c)
        elif pd.api.types.is_datetime64_any_dtype(dt):
            # evitar datetime raw en estimadores
            continue
        else:
            numerical.append(c)

    feature_cols = categorical + numerical
    return feature_cols, categorical, numerical


def _prepare_xy(
    df: pd.DataFrame,
    feature_cols: list[str],
    categorical: list[str],
    numerical: list[str],
) -> pd.DataFrame:
    X = df.copy()

    for c in feature_cols:
        if c not in X.columns:
            if c in categorical:
                X[c] = "MISSING"
            else:
                X[c] = np.nan

    X = X[feature_cols].copy()

    for c in categorical:
        X[c] = X[c].astype(str).fillna("MISSING")

    for c in numerical:
        X[c] = pd.to_numeric(X[c], errors="coerce").astype(float)

    return X


def _build_preprocessor(categorical: list[str], numerical: list[str]) -> ColumnTransformer:
    transformers = []
    if categorical:
        transformers.append(
            (
                "cat",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                categorical,
            )
        )
    if numerical:
        transformers.append(("num", "passthrough", numerical))
    return ColumnTransformer(transformers=transformers, remainder="drop")


def _make_regressor(quantile: float | None = None, random_state: int = 42):
    if HAS_LIGHTGBM:
        params = {
            "n_estimators": 200,
            "learning_rate": 0.03,
            "num_leaves": 31,
            "subsample": 0.9,
            "colsample_bytree": 0.9,
            "random_state": random_state,
            "n_jobs": -1,
        }
        if quantile is None:
            return LGBMRegressor(objective="regression", **params), "lightgbm"
        return LGBMRegressor(objective="quantile", alpha=quantile, **params), "lightgbm"

    if quantile is None:
        return (
            HistGradientBoostingRegressor(
                loss="squared_error",
                random_state=random_state,
                max_depth=8,
                max_iter=250,
                learning_rate=0.05,
            ),
            "histgbr",
        )

    return (
        HistGradientBoostingRegressor(
            loss="quantile",
            quantile=quantile,
            random_state=random_state,
            max_depth=8,
            max_iter=250,
            learning_rate=0.05,
        ),
        "histgbr",
    )


def fit_target_models(
    train_df: pd.DataFrame,
    target: str,
    feature_cols: list[str],
    categorical: list[str],
    numerical: list[str],
) -> dict:
    df = train_df.dropna(subset=[target]).copy()
    X = _prepare_xy(df, feature_cols, categorical, numerical)
    y = pd.to_numeric(df[target], errors="coerce").fillna(0.0).astype(float)

    pre = _build_preprocessor(categorical, numerical)

    reg50, backend = _make_regressor(quantile=0.5)
    reg80, _ = _make_regressor(quantile=0.8)

    model50 = Pipeline([("prep", pre), ("model", reg50)])
    model80 = Pipeline([("prep", _build_preprocessor(categorical, numerical)), ("model", reg80)])

    fallback = None
    try:
        model50.fit(X, y)
        model80.fit(X, y)
    except Exception as exc:
        LOGGER.warning("Fallo entrenamiento quantile para %s, fallback por residuos: %s", target, exc)
        reg_mean, backend = _make_regressor(quantile=None)
        model_mean = Pipeline([("prep", _build_preprocessor(categorical, numerical)), ("model", reg_mean)])
        model_mean.fit(X, y)
        pred_train = model_mean.predict(X)
        residuals = y - pred_train
        q80 = float(np.quantile(residuals, 0.8)) if len(residuals) else 0.0
        model50 = model_mean
        model80 = None
        fallback = {"resid_q80": q80}

    artifact = {
        "target": target,
        "feature_cols": feature_cols,
        "categorical_cols": categorical,
        "numerical_cols": numerical,
        "backend": backend,
        "model_p50": model50,
        "model_p80": model80,
        "fallback": fallback,
    }
    return artifact


def predict_with_artifact(artifact: dict, df: pd.DataFrame) -> pd.DataFrame:
    X = _prepare_xy(
        df,
        artifact["feature_cols"],
        artifact["categorical_cols"],
        artifact["numerical_cols"],
    )
    p50 = artifact["model_p50"].predict(X)

    if artifact.get("model_p80") is not None:
        p80 = artifact["model_p80"].predict(X)
    else:
        shift = artifact.get("fallback", {}).get("resid_q80", 0.0)
        p80 = p50 + shift

    pred = pd.DataFrame({"pred_p50": p50, "pred_p80": p80})
    pred["pred_p80"] = np.maximum(pred["pred_p80"], pred["pred_p50"])
    return pred


def train_and_save_models(
    train_df: pd.DataFrame,
    target_cols: list[str],
    axis: str,
    freq: str,
    model_dir: Path,
) -> list[ModelArtifact]:
    model_dir.mkdir(parents=True, exist_ok=True)

    feature_cols, categorical, numerical = build_feature_sets(train_df, target_cols)

    artifacts = []
    for target in target_cols:
        LOGGER.info("Entrenando modelo %s | axis=%s | freq=%s", target, axis, freq)
        art = fit_target_models(train_df, target, feature_cols, categorical, numerical)
        path = model_dir / f"{axis}_{freq}_{target}.joblib"
        joblib.dump(art, path)
        artifacts.append(ModelArtifact(target=target, axis=axis, freq=freq, path=path))

    return artifacts


def load_model_artifacts(model_artifacts: list[ModelArtifact]) -> dict[str, dict]:
    out = {}
    for m in model_artifacts:
        out[m.target] = joblib.load(m.path)
    return out
