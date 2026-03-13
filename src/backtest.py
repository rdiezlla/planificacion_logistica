from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from .train import build_feature_sets, fit_target_models, predict_with_artifact

LOGGER = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    min_train_days: int = 180
    test_days: int = 28
    step_days: int = 28
    max_folds: int = 8


def wape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    denom = np.sum(np.abs(y_true))
    if denom == 0:
        return np.nan
    return float(np.sum(np.abs(y_true - y_pred)) / denom)


def smape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    denom = np.abs(y_true) + np.abs(y_pred)
    denom = np.where(denom == 0, 1e-8, denom)
    return float(np.mean(2.0 * np.abs(y_true - y_pred) / denom))


def mase(y_true: np.ndarray, y_pred: np.ndarray, mae_naive: float) -> float:
    if mae_naive <= 1e-12 or np.isnan(mae_naive):
        return np.nan
    return float(np.mean(np.abs(y_true - y_pred)) / mae_naive)


def pinball_loss(y_true: np.ndarray, y_pred: np.ndarray, quantile: float) -> float:
    diff = y_true - y_pred
    loss = np.maximum(quantile * diff, (quantile - 1.0) * diff)
    return float(np.mean(loss)) if len(loss) else np.nan


def _mae_seasonal_naive(train_y: pd.Series, seasonality: int = 7) -> float:
    if len(train_y) <= seasonality:
        return np.nan
    diff = np.abs(train_y.iloc[seasonality:].to_numpy() - train_y.iloc[:-seasonality].to_numpy())
    if len(diff) == 0:
        return np.nan
    return float(np.mean(diff))


def _baseline_forecasts(train_y: pd.Series, test_y: pd.Series) -> pd.DataFrame:
    history = list(train_y.astype(float).to_numpy())
    naive7, ma7, ma28 = [], [], []

    for actual in test_y.astype(float).to_numpy():
        if len(history) >= 7:
            naive7.append(history[-7])
            ma7.append(float(np.mean(history[-7:])))
        else:
            naive7.append(float(np.mean(history)) if history else 0.0)
            ma7.append(float(np.mean(history)) if history else 0.0)

        if len(history) >= 28:
            ma28.append(float(np.mean(history[-28:])))
        else:
            ma28.append(float(np.mean(history)) if history else 0.0)

        history.append(actual)

    return pd.DataFrame({"naive_t7": naive7, "ma7": ma7, "ma28": ma28})


def _compute_metrics_row(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    mae_naive: float,
    target: str,
    model_name: str,
    fold_id: int,
    fold_start: pd.Timestamp,
    fold_end: pd.Timestamp,
    axis: str,
    freq: str,
    quantile_eval: float | None = None,
) -> dict:
    row = {
        "axis": axis,
        "freq": freq,
        "target": target,
        "model": model_name,
        "fold_id": fold_id,
        "fold_start": fold_start,
        "fold_end": fold_end,
        "wape": wape(y_true, y_pred),
        "smape": smape(y_true, y_pred),
        "mase": mase(y_true, y_pred, mae_naive),
        "empirical_coverage_p80": float(np.mean(y_true <= y_pred)) if quantile_eval == 0.8 else np.nan,
        "pinball_loss_p50": pinball_loss(y_true, y_pred, 0.5) if quantile_eval == 0.5 else np.nan,
        "pinball_loss_p80": pinball_loss(y_true, y_pred, 0.8) if quantile_eval == 0.8 else np.nan,
    }

    if len(y_true) > 0:
        thr = np.quantile(y_true, 0.95)
        peak_mask = y_true >= thr
        if peak_mask.any():
            row["wape_peak5"] = wape(y_true[peak_mask], y_pred[peak_mask])
            row["smape_peak5"] = smape(y_true[peak_mask], y_pred[peak_mask])
            row["mase_peak5"] = mase(y_true[peak_mask], y_pred[peak_mask], mae_naive)
        else:
            row["wape_peak5"] = np.nan
            row["smape_peak5"] = np.nan
            row["mase_peak5"] = np.nan
    else:
        row["wape_peak5"] = np.nan
        row["smape_peak5"] = np.nan
        row["mase_peak5"] = np.nan

    return row


def _baseline_by_segment(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    target: str,
    segment_cols: list[str],
) -> pd.DataFrame:
    out = pd.DataFrame(index=test_df.index, columns=["naive_t7", "ma7", "ma28"], dtype=float)

    if not segment_cols:
        b = _baseline_forecasts(train_df[target], test_df[target])
        out.loc[test_df.index, ["naive_t7", "ma7", "ma28"]] = b.to_numpy()
        return out

    for seg, g_test in test_df.groupby(segment_cols, dropna=False):
        if not isinstance(seg, tuple):
            seg = (seg,)

        mask = pd.Series(True, index=train_df.index)
        for c, v in zip(segment_cols, seg):
            mask &= train_df[c].eq(v)

        g_train = train_df.loc[mask].sort_values("date")
        g_test = g_test.sort_values("date")

        if g_train.empty:
            # fallback de segmento sin histórico: promedio global del train
            global_mean = float(train_df[target].mean()) if len(train_df) else 0.0
            b = pd.DataFrame(
                {
                    "naive_t7": np.full(len(g_test), global_mean),
                    "ma7": np.full(len(g_test), global_mean),
                    "ma28": np.full(len(g_test), global_mean),
                },
                index=g_test.index,
            )
        else:
            b = _baseline_forecasts(g_train[target], g_test[target])
            b.index = g_test.index

        out.loc[g_test.index, ["naive_t7", "ma7", "ma28"]] = b[["naive_t7", "ma7", "ma28"]]

    return out


def run_backtest(
    df_hist: pd.DataFrame,
    target_cols: list[str],
    axis: str,
    freq: str,
    segment_cols: list[str] | None = None,
    config: BacktestConfig | None = None,
) -> pd.DataFrame:
    cfg = config or BacktestConfig()
    segment_cols = segment_cols or []

    data = df_hist.copy().sort_values("date")
    dates = sorted(data["date"].dropna().unique())
    if len(dates) < cfg.min_train_days + cfg.test_days:
        LOGGER.warning(
            "Sin suficientes dias para backtest (%s/%s): axis=%s freq=%s",
            len(dates),
            cfg.min_train_days + cfg.test_days,
            axis,
            freq,
        )
        return pd.DataFrame()

    fold_starts = list(range(cfg.min_train_days, len(dates) - cfg.test_days + 1, cfg.step_days))
    if len(fold_starts) > cfg.max_folds:
        fold_starts = fold_starts[-cfg.max_folds :]

    metrics_rows = []

    for fold_id, start_idx in enumerate(fold_starts, start=1):
        train_end = dates[start_idx - 1]
        test_start = dates[start_idx]
        test_end = dates[start_idx + cfg.test_days - 1]

        train = data[data["date"] <= train_end].copy()
        test = data[(data["date"] >= test_start) & (data["date"] <= test_end)].copy()

        if train.empty or test.empty:
            continue

        for target in target_cols:
            feature_cols, cat_cols, num_cols = build_feature_sets(train, [target])

            baselines = _baseline_by_segment(train, test, target, segment_cols)
            test_eval = test.copy()
            test_eval[["naive_t7", "ma7", "ma28"]] = baselines[["naive_t7", "ma7", "ma28"]]

            y_true = pd.to_numeric(test_eval[target], errors="coerce").fillna(0.0).to_numpy(dtype=float)
            mae_naive = _mae_seasonal_naive(pd.to_numeric(train[target], errors="coerce").fillna(0.0), seasonality=7)

            for baseline_name in ["naive_t7", "ma7", "ma28"]:
                y_pred = pd.to_numeric(test_eval[baseline_name], errors="coerce").fillna(0.0).to_numpy(dtype=float)
                metrics_rows.append(
                    _compute_metrics_row(
                        y_true=y_true,
                        y_pred=y_pred,
                        mae_naive=mae_naive,
                        target=target,
                        model_name=baseline_name,
                        fold_id=fold_id,
                        fold_start=pd.Timestamp(test_start),
                        fold_end=pd.Timestamp(test_end),
                        axis=axis,
                        freq=freq,
                        quantile_eval=0.5,
                    )
                )

            artifact = fit_target_models(train, target, feature_cols, cat_cols, num_cols)
            pred = predict_with_artifact(artifact, test)
            y_pred_model = pred["pred_p50"].to_numpy(dtype=float)
            metrics_rows.append(
                _compute_metrics_row(
                    y_true=y_true,
                    y_pred=y_pred_model,
                    mae_naive=mae_naive,
                    target=target,
                    model_name="model_p50",
                    fold_id=fold_id,
                    fold_start=pd.Timestamp(test_start),
                    fold_end=pd.Timestamp(test_end),
                    axis=axis,
                    freq=freq,
                    quantile_eval=0.5,
                )
            )

            y_pred_q80 = pred["pred_p80"].to_numpy(dtype=float)
            metrics_rows.append(
                _compute_metrics_row(
                    y_true=y_true,
                    y_pred=y_pred_q80,
                    mae_naive=mae_naive,
                    target=target,
                    model_name="model_p80",
                    fold_id=fold_id,
                    fold_start=pd.Timestamp(test_start),
                    fold_end=pd.Timestamp(test_end),
                    axis=axis,
                    freq=freq,
                    quantile_eval=0.8,
                )
            )

    return pd.DataFrame(metrics_rows)
