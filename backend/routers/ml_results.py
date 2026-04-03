"""
ml_results.py — ML experiment results API endpoints.

Reads experiment logs and evaluation reports from the experiments/ directory
and serves them to the frontend for the ML Dashboard.

Endpoints:
    GET /api/ml/experiments         — list all experiment runs
    GET /api/ml/experiments/{id}    — full experiment detail
    GET /api/ml/latest-model        — info about the most recent model
"""

import json
import os
from glob import glob
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

# Path to experiments directory
EXPERIMENTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "ml", "experiments"
)


class ExperimentSummary(BaseModel):
    """Summary of one experiment run."""
    experiment_id: str
    timestamp: Optional[str] = None
    best_val_loss: Optional[float] = None
    epochs_completed: Optional[int] = None
    device: Optional[str] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1: Optional[float] = None


class ExperimentDetail(BaseModel):
    """Full experiment detail including losses and hyperparameters."""
    experiment_id: str
    timestamp: Optional[str] = None
    hyperparameters: Optional[dict] = None
    dataset: Optional[dict] = None
    results: Optional[dict] = None
    losses: Optional[dict] = None
    evaluation: Optional[dict] = None


class LatestModel(BaseModel):
    """Info about the most recently trained model."""
    experiment_id: str
    model_path: str
    best_val_loss: Optional[float] = None
    evaluation_metrics: Optional[dict] = None


def _load_experiment(experiment_id: str) -> dict:
    """Load experiment.json for a given experiment."""
    exp_dir = os.path.join(EXPERIMENTS_DIR, experiment_id)
    log_path = os.path.join(exp_dir, "experiment.json")

    if not os.path.exists(log_path):
        raise HTTPException(status_code=404, detail=f"Experiment '{experiment_id}' not found")

    with open(log_path) as f:
        return json.load(f)


def _load_evaluation(experiment_id: str) -> Optional[dict]:
    """Load evaluation.json for a given experiment (may not exist yet)."""
    eval_path = os.path.join(EXPERIMENTS_DIR, experiment_id, "evaluation.json")
    if os.path.exists(eval_path):
        with open(eval_path) as f:
            return json.load(f)
    return None


@router.get("/experiments", response_model=list[ExperimentSummary])
def list_experiments():
    """List all experiment runs with summary metrics."""
    experiments = []

    exp_dirs = sorted(glob(os.path.join(EXPERIMENTS_DIR, "exp_*")))
    for exp_dir in exp_dirs:
        experiment_id = os.path.basename(exp_dir)
        log_path = os.path.join(exp_dir, "experiment.json")

        if not os.path.exists(log_path):
            continue

        with open(log_path) as f:
            log = json.load(f)

        # Load evaluation if available
        eval_data = _load_evaluation(experiment_id)

        summary = ExperimentSummary(
            experiment_id=experiment_id,
            timestamp=log.get("timestamp"),
            best_val_loss=log.get("results", {}).get("best_val_loss"),
            epochs_completed=log.get("hyperparameters", {}).get("epochs_completed"),
            device=log.get("results", {}).get("device"),
            precision=eval_data.get("metrics", {}).get("precision") if eval_data else None,
            recall=eval_data.get("metrics", {}).get("recall") if eval_data else None,
            f1=eval_data.get("metrics", {}).get("f1") if eval_data else None,
        )
        experiments.append(summary)

    return experiments


@router.get("/experiments/{experiment_id}", response_model=ExperimentDetail)
def get_experiment(experiment_id: str):
    """Get full detail for a specific experiment."""
    log = _load_experiment(experiment_id)
    eval_data = _load_evaluation(experiment_id)

    return ExperimentDetail(
        experiment_id=experiment_id,
        timestamp=log.get("timestamp"),
        hyperparameters=log.get("hyperparameters"),
        dataset=log.get("dataset"),
        results=log.get("results"),
        losses=log.get("losses"),
        evaluation=eval_data,
    )


@router.get("/latest-model", response_model=LatestModel)
def get_latest_model():
    """Get info about the most recently trained model."""
    exp_dirs = sorted(glob(os.path.join(EXPERIMENTS_DIR, "exp_*")))

    if not exp_dirs:
        raise HTTPException(status_code=404, detail="No trained models found")

    latest_dir = exp_dirs[-1]
    experiment_id = os.path.basename(latest_dir)
    model_path = os.path.join(latest_dir, "model.pt")

    log = _load_experiment(experiment_id)
    eval_data = _load_evaluation(experiment_id)

    return LatestModel(
        experiment_id=experiment_id,
        model_path=model_path,
        best_val_loss=log.get("results", {}).get("best_val_loss"),
        evaluation_metrics=eval_data.get("metrics") if eval_data else None,
    )
