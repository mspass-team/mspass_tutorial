#!/usr/bin/env python3
"""Compare PhaseNet-NCEDC and PhaseNet-USArray on the sample USArray dataset."""

from __future__ import annotations

import argparse
import json
import random
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import torch
from torch.utils.data import DataLoader

import seisbench.data as sbd
import seisbench.generate as sbg
import seisbench.models as sbm
from seisbench.util import worker_seeding


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_DATASET = SCRIPT_DIR / "usarray_samples"
DEFAULT_USARRAY_CKPT = SCRIPT_DIR / "checkpoint" / "phasenet-usarray.pt"

PHASE_DICT = {
    "trace_p_arrival_sample": "P",
    "trace_pP_arrival_sample": "P",
    "trace_P_arrival_sample": "P",
    "trace_P1_arrival_sample": "P",
    "trace_Pg_arrival_sample": "P",
    "trace_Pn_arrival_sample": "P",
    "trace_PmP_arrival_sample": "P",
    "trace_pwP_arrival_sample": "P",
    "trace_pwPm_arrival_sample": "P",
    "trace_s_arrival_sample": "S",
    "trace_S_arrival_sample": "S",
    "trace_S1_arrival_sample": "S",
    "trace_Sg_arrival_sample": "S",
    "trace_SmS_arrival_sample": "S",
    "trace_Sn_arrival_sample": "S",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare PhaseNet-USArray and SeisBench PhaseNet-NCEDC."
    )
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--years", nargs="+", default=None)
    parser.add_argument("--usarray-checkpoint", type=Path, default=DEFAULT_USARRAY_CKPT)
    parser.add_argument(
        "--pretrained-name",
        default="original",
        help="SeisBench PhaseNet pretrained key. 'original' is the NCEDC-trained PhaseNet.",
    )
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--num-workers", type=int, default=0)
    parser.add_argument("--device", choices=("auto", "cpu", "cuda", "mps"), default="auto")
    parser.add_argument("--prob-threshold", type=float, default=0.5)
    parser.add_argument("--match-window-s", type=float, default=0.1)
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--seed", type=int, default=20260426)
    parser.add_argument("--out-json", type=Path, default=SCRIPT_DIR / "phasenet_comparison_metrics.json")
    return parser.parse_args()


def resolve_device(name: str) -> torch.device:
    if name == "auto":
        if torch.cuda.is_available():
            return torch.device("cuda")
        if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
            return torch.device("mps")
        return torch.device("cpu")
    return torch.device(name)


def set_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def available_years(dataset_root: Path) -> list[str]:
    years = sorted(p.name for p in dataset_root.iterdir() if p.is_dir())
    if not years:
        raise FileNotFoundError(f"No year directories found under {dataset_root}")
    return years


def build_loader(
    dataset_root: Path,
    years: list[str],
    batch_size: int,
    num_workers: int,
) -> tuple[DataLoader, int]:
    datasets = [sbd.WaveformDataset(dataset_root / year) for year in years]
    ds = sbd.MultiWaveformDataset(datasets)
    _, _, test = ds.train_dev_test()

    generator = sbg.GenericGenerator(test)
    generator.add_augmentations(
        [
            sbg.WindowAroundSample(
                list(PHASE_DICT.keys()),
                samples_before=3000,
                windowlen=6000,
                selection="random",
                strategy="variable",
            ),
            sbg.RandomWindow(windowlen=3001, strategy="pad"),
            sbg.ChangeDtype(np.float32),
            sbg.ProbabilisticLabeller(
                label_columns=PHASE_DICT,
                model_labels="PSN",
                sigma=30,
                dim=0,
            ),
        ]
    )

    loader = DataLoader(
        generator,
        batch_size=batch_size,
        shuffle=False,
        num_workers=num_workers,
        worker_init_fn=worker_seeding if num_workers else None,
    )
    return loader, len(test)


def build_usarray_model(checkpoint: Path) -> torch.nn.Module:
    if not checkpoint.exists():
        raise FileNotFoundError(f"USArray checkpoint not found: {checkpoint}")
    model = sbm.PhaseNet(
        phases="PSN",
        norm="std",
        default_args={"blinding": (200, 200)},
    )
    state = torch.load(checkpoint, map_location="cpu")
    if isinstance(state, dict) and "state_dict" in state:
        state = state["state_dict"]
    model.load_state_dict(state, strict=False)
    return model


def build_pretrained_model(name: str) -> torch.nn.Module:
    try:
        return sbm.PhaseNet.from_pretrained(name)
    except TypeError:
        return sbm.PhaseNet.from_pretrained(name, update=False)


def label_index_map(labels: Any) -> dict[str, int]:
    label_string = "".join(labels) if isinstance(labels, (list, tuple)) else str(labels)
    return {phase: label_string.index(phase) for phase in ("P", "S") if phase in label_string}


def empty_phase_stats() -> dict[str, Any]:
    return {
        "tp": 0,
        "fp": 0,
        "fn": 0,
        "precision": 0.0,
        "recall": 0.0,
        "f1": 0.0,
        "mu_ms": 0.0,
        "sigma_ms": 0.0,
        "residual_count": 0,
    }


@torch.inference_mode()
def preload_batches(loader: DataLoader, max_batches: int | None) -> tuple[list[dict[str, torch.Tensor]], int]:
    batches: list[dict[str, torch.Tensor]] = []
    total_samples = 0
    for batch_idx, batch in enumerate(loader):
        if max_batches is not None and batch_idx >= max_batches:
            break
        kept = {
            "X": batch["X"].detach().cpu().float(),
            "y": batch["y"].detach().cpu().float(),
        }
        batches.append(kept)
        total_samples += int(kept["X"].shape[0])
    return batches, total_samples


def evaluate_model(
    model: torch.nn.Module,
    batches: list[dict[str, torch.Tensor]],
    device: torch.device,
    prob_threshold: float,
    match_window_s: float,
) -> dict[str, Any]:
    model.to(device)
    model.eval()

    true_phase_to_idx = {"P": 0}  # y labels are "PSN"; only P is reported here.
    pred_phase_to_idx = label_index_map(getattr(model, "labels", "PSN"))
    sample_rate = float(getattr(model, "sampling_rate", 100.0))
    dt = 1.0 / sample_rate
    match_window_samples = match_window_s / dt

    tp: dict[str, int] = defaultdict(int)
    fp: dict[str, int] = defaultdict(int)
    fn: dict[str, int] = defaultdict(int)
    residuals: dict[str, list[float]] = {"P": []}
    total_samples = 0

    for batch in batches:
        x = batch["X"].to(device)
        y = batch["y"].to(device)
        if hasattr(model, "annotate_batch_pre"):
            x = model.annotate_batch_pre(x, {})

        pred = model(x)
        total_samples += int(x.shape[0])

        y_true = y.detach().cpu()
        y_pred = pred.detach().cpu()

        for phase in ("P",):
            if phase not in pred_phase_to_idx:
                continue

            true_dist = y_true[:, true_phase_to_idx[phase], :]
            pred_dist = y_pred[:, pred_phase_to_idx[phase], :]

            gt_has_pick = true_dist.max(dim=-1).values > 0.0
            gt_pick_idx = true_dist.argmax(dim=-1)
            pred_max_vals, pred_pick_idx = pred_dist.max(dim=-1)
            pred_has_pick = pred_max_vals >= prob_threshold

            for i in range(y_true.shape[0]):
                has_gt = bool(gt_has_pick[i])
                has_pred = bool(pred_has_pick[i])

                if not has_gt and not has_pred:
                    continue
                if not has_gt and has_pred:
                    fp[phase] += 1
                    continue
                if has_gt and not has_pred:
                    fn[phase] += 1
                    continue

                residual_samples = int(pred_pick_idx[i]) - int(gt_pick_idx[i])
                if abs(residual_samples) <= match_window_samples:
                    tp[phase] += 1
                    residuals[phase].append(residual_samples * dt)
                else:
                    fp[phase] += 1
                    fn[phase] += 1

    metrics: dict[str, Any] = {}
    for phase in ("P",):
        phase_metrics = empty_phase_stats()
        phase_metrics["tp"] = int(tp[phase])
        phase_metrics["fp"] = int(fp[phase])
        phase_metrics["fn"] = int(fn[phase])

        precision = tp[phase] / (tp[phase] + fp[phase]) if tp[phase] + fp[phase] else 0.0
        recall = tp[phase] / (tp[phase] + fn[phase]) if tp[phase] + fn[phase] else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        residual_array = np.asarray(residuals[phase], dtype=np.float32)

        phase_metrics.update(
            {
                "precision": float(precision),
                "recall": float(recall),
                "f1": float(f1),
                "mu_ms": float(residual_array.mean() * 1000.0) if residual_array.size else 0.0,
                "sigma_ms": float(residual_array.std() * 1000.0) if residual_array.size else 0.0,
                "residual_count": int(residual_array.size),
            }
        )
        metrics[phase] = phase_metrics

    return {
        "samples_evaluated": total_samples,
        "model_labels": list(getattr(model, "labels", [])),
        "metrics_by_phase": metrics,
    }


def print_summary(name: str, result: dict[str, Any]) -> None:
    print(f"\n{name}")
    print(f"  labels: {''.join(result['model_labels'])}")
    print(f"  samples: {result['samples_evaluated']}")
    for phase, metrics in result["metrics_by_phase"].items():
        print(
            f"  {phase}: TP={metrics['tp']} FP={metrics['fp']} FN={metrics['fn']} "
            f"P={metrics['precision']:.3f} R={metrics['recall']:.3f} "
            f"F1={metrics['f1']:.3f} "
            f"mu={metrics['mu_ms']:.2f} ms sigma={metrics['sigma_ms']:.2f} ms"
        )


def metric_delta(
    results: dict[str, dict[str, Any]],
    baseline: str = "PhaseNet-NCEDC",
    candidate: str = "PhaseNet-USArray",
) -> dict[str, dict[str, Any]]:
    comparison: dict[str, dict[str, Any]] = {}
    baseline_metrics = results[baseline]["metrics_by_phase"]
    candidate_metrics = results[candidate]["metrics_by_phase"]

    for phase in ("P",):
        phase_comparison: dict[str, Any] = {}
        for metric in ("precision", "recall", "f1"):
            base_value = float(baseline_metrics[phase][metric])
            candidate_value = float(candidate_metrics[phase][metric])
            absolute_delta = candidate_value - base_value
            relative_delta = (
                absolute_delta / base_value if abs(base_value) > 1e-12 else None
            )
            phase_comparison[metric] = {
                "baseline": base_value,
                "candidate": candidate_value,
                "absolute_delta": absolute_delta,
                "relative_delta": relative_delta,
                "percent_change": 100.0 * relative_delta if relative_delta is not None else None,
            }
        comparison[phase] = phase_comparison
    return comparison


def format_percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{100.0 * value:+.1f}%"


def print_metric_comparison(
    results: dict[str, dict[str, Any]],
    comparison: dict[str, dict[str, Any]],
    baseline: str = "PhaseNet-NCEDC",
    candidate: str = "PhaseNet-USArray",
) -> None:
    print("\nPrecision / Recall / F1 Comparison")
    print(f"Baseline:  {baseline}")
    print(f"Candidate: {candidate}")
    header = (
        f"{'Phase':<5} {'Metric':<10} "
        f"{baseline:<18} {candidate:<18} {'Delta':<10} {'Relative':<10}"
    )
    print(header)
    print("-" * len(header))

    for phase in ("P",):
        for metric in ("precision", "recall", "f1"):
            item = comparison[phase][metric]
            print(
                f"{phase:<5} {metric:<10} "
                f"{item['baseline']:<18.3f} "
                f"{item['candidate']:<18.3f} "
                f"{item['absolute_delta']:+.3f}     "
                f"{format_percent(item['relative_delta']):<10}"
            )

    p_f1_delta = comparison["P"]["f1"]["absolute_delta"]
    p_recall_delta = comparison["P"]["recall"]["absolute_delta"]
    p_precision_delta = comparison["P"]["precision"]["absolute_delta"]
    p_f1_pct = comparison["P"]["f1"]["percent_change"]
    p_recall_pct = comparison["P"]["recall"]["percent_change"]
    p_precision_pct = comparison["P"]["precision"]["percent_change"]
    print(
        "\nP-phase headline: "
        f"F1 {p_f1_delta:+.3f} ({format_percent(None if p_f1_pct is None else p_f1_pct / 100.0)}), "
        f"recall {p_recall_delta:+.3f} ({format_percent(None if p_recall_pct is None else p_recall_pct / 100.0)}), "
        f"precision {p_precision_delta:+.3f} ({format_percent(None if p_precision_pct is None else p_precision_pct / 100.0)}) "
        f"({candidate} minus {baseline})."
    )


def main() -> None:
    args = parse_args()
    set_seed(args.seed)
    device = resolve_device(args.device)
    years = args.years or available_years(args.dataset)

    loader, n_test = build_loader(
        args.dataset,
        years,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
    )
    batches, n_eval = preload_batches(loader, args.max_batches)
    if not batches:
        raise RuntimeError("No evaluation batches were produced.")

    models = {
        "PhaseNet-NCEDC": build_pretrained_model(args.pretrained_name),
        "PhaseNet-USArray": build_usarray_model(args.usarray_checkpoint),
    }

    results = {}
    for name, model in models.items():
        set_seed(args.seed)
        results[name] = evaluate_model(
            model,
            batches,
            device=device,
            prob_threshold=args.prob_threshold,
            match_window_s=args.match_window_s,
        )
        print_summary(name, results[name])

    comparison = metric_delta(results)
    print_metric_comparison(results, comparison)

    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset": str(args.dataset),
        "years": years,
        "test_samples_available": n_test,
        "samples_evaluated": n_eval,
        "device": str(device),
        "prob_threshold": args.prob_threshold,
        "match_window_s": args.match_window_s,
        "pretrained_name": args.pretrained_name,
        "usarray_checkpoint": str(args.usarray_checkpoint),
        "results": results,
        "comparison": comparison,
    }
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    with args.out_json.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(f"\nWrote {args.out_json}")


if __name__ == "__main__":
    main()
