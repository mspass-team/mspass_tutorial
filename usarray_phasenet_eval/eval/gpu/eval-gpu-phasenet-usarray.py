#!/usr/bin/env python3
"""Measure model-only GPU inference throughput for PhaseNet-USArray."""

from __future__ import annotations

import sys
from pathlib import Path

import torch
import seisbench.models as sbm


SCRIPT_DIR = Path(__file__).resolve().parent
EVAL_DIR = SCRIPT_DIR.parent
PROJECT_DIR = EVAL_DIR.parent
for path in (EVAL_DIR, PROJECT_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from common import run_benchmark


DEFAULT_CHECKPOINT = SCRIPT_DIR / "new_phasenet_gpu_original.pt"
DEFAULT_OUT = SCRIPT_DIR / "throughput_model_only_usarray.json"


def build_model(checkpoint: Path) -> torch.nn.Module:
    model = sbm.PhaseNet(
        phases="PSN",
        norm="std",
        default_args={"blinding": (200, 200)},
    )
    if not checkpoint.exists():
        raise FileNotFoundError(f"Checkpoint not found: {checkpoint}")
    state = torch.load(checkpoint, map_location="cpu")
    model.load_state_dict(state, strict=False)
    return model


def main() -> None:
    run_benchmark(
        script_path=Path(__file__),
        model_name="usarray",
        device_mode="gpu",
        default_checkpoint=DEFAULT_CHECKPOINT,
        default_out=DEFAULT_OUT,
        description="Measure PhaseNet-USArray model-only GPU throughput.",
        build_model=build_model,
    )


if __name__ == "__main__":
    main()
