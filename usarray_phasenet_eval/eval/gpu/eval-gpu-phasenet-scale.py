#!/usr/bin/env python3
"""Measure model-only GPU inference throughput for PhaseNet-Scale."""

from __future__ import annotations

import sys
from pathlib import Path

import torch


SCRIPT_DIR = Path(__file__).resolve().parent
EVAL_DIR = SCRIPT_DIR.parent
PROJECT_DIR = EVAL_DIR.parent
for path in (EVAL_DIR, PROJECT_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import phasenet_scale
from common import run_benchmark


DEFAULT_CHECKPOINT = SCRIPT_DIR / "new_phasenet_best_8_factor_1129.pt"
DEFAULT_OUT = SCRIPT_DIR / "throughput_model_only_scale.json"


def build_model(checkpoint: Path) -> torch.nn.Module:
    model = phasenet_scale.PhaseNet(
        phases="NPS",
        norm="std",
        filter_factor=8,
        use_aspp=True,
        use_se=True,
        se_reduction=8,
    )
    if not checkpoint.exists():
        raise FileNotFoundError(f"Checkpoint not found: {checkpoint}")
    state = torch.load(checkpoint, map_location="cpu")
    model.load_state_dict(state, strict=False)
    return model


def main() -> None:
    run_benchmark(
        script_path=Path(__file__),
        model_name="scale",
        device_mode="gpu",
        default_checkpoint=DEFAULT_CHECKPOINT,
        default_out=DEFAULT_OUT,
        description="Measure PhaseNet-Scale model-only GPU throughput.",
        build_model=build_model,
    )


if __name__ == "__main__":
    main()
