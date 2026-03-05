from __future__ import annotations

from pathlib import Path

import pandas as pd

from .train import ModelArtifact, load_model_artifacts, predict_with_artifact


def predict_targets_wide(
    data: pd.DataFrame,
    model_artifacts: list[ModelArtifact],
) -> pd.DataFrame:
    models = load_model_artifacts(model_artifacts)
    out = data.copy()

    for target, artifact in models.items():
        pred = predict_with_artifact(artifact, out)
        out[f"{target}_p50"] = pred["pred_p50"]
        out[f"{target}_p80"] = pred["pred_p80"]

    return out


def collect_model_paths(model_artifacts: list[ModelArtifact]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "axis": m.axis,
                "freq": m.freq,
                "target": m.target,
                "model_path": str(Path(m.path).resolve()),
            }
            for m in model_artifacts
        ]
    )
