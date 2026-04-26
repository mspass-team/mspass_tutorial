"""Shared model-only throughput benchmark helpers."""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import torch
from torch.utils.data import DataLoader

import seisbench.data as sbd
import seisbench.generate as sbg
from seisbench.util import worker_seeding


YEAR_LIST = ["2007", "2008", "2011", "2012", "2013"]

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


def parse_args(
    *,
    description: str,
    default_checkpoint: Path,
    default_out: Path,
    device_mode: str,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--outroot", type=Path, default=Path("./seisbench_usarray"))
    parser.add_argument("--years", nargs="+", default=YEAR_LIST)
    parser.add_argument("--checkpoint", type=Path, default=default_checkpoint)
    parser.add_argument("--batch-size", type=int, default=512)
    parser.add_argument("--num-workers", type=int, default=8)
    parser.add_argument("--max-batches", type=int, default=50)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--out", type=Path, default=default_out)
    parser.add_argument(
        "--include-labels",
        action="store_true",
        help="Also build probabilistic labels in the dataloader. Labels are not used for timing.",
    )

    if device_mode == "cpu":
        parser.add_argument("--torch-threads", type=int, default=None)
        parser.add_argument("--torch-interop-threads", type=int, default=None)
    else:
        parser.add_argument("--device", choices=["cuda", "cpu"], default="cuda")

    return parser.parse_args()


def configure_torch_threads(args: argparse.Namespace) -> None:
    if getattr(args, "torch_threads", None) is not None:
        torch.set_num_threads(args.torch_threads)
    if getattr(args, "torch_interop_threads", None) is not None:
        torch.set_num_interop_threads(args.torch_interop_threads)


def build_eval_loader(
    *,
    outroot: Path,
    years: list[str],
    batch_size: int,
    num_workers: int,
    include_labels: bool,
    model_labels: Any,
    pin_memory: bool,
) -> DataLoader:
    datasets = [sbd.WaveformDataset(outroot / year) for year in years]
    ds = sbd.MultiWaveformDataset(datasets)
    _, _, test = ds.train_dev_test()

    generator = sbg.GenericGenerator(test)
    augmentations = [
        sbg.WindowAroundSample(
            list(PHASE_DICT.keys()),
            samples_before=3000,
            windowlen=6000,
            selection="random",
            strategy="variable",
        ),
        sbg.RandomWindow(windowlen=3001, strategy="pad"),
        sbg.ChangeDtype(np.float32),
    ]
    if include_labels:
        augmentations.append(
            sbg.ProbabilisticLabeller(
                label_columns=PHASE_DICT,
                model_labels=model_labels,
                sigma=30,
                dim=0,
            )
        )
    generator.add_augmentations(augmentations)

    return DataLoader(
        generator,
        batch_size=batch_size,
        shuffle=False,
        num_workers=num_workers,
        worker_init_fn=worker_seeding if num_workers else None,
        pin_memory=pin_memory,
    )


def model_info(model: torch.nn.Module, checkpoint: Path) -> dict[str, Any]:
    param_bytes = sum(p.nelement() * p.element_size() for p in model.parameters())
    buffer_bytes = sum(b.nelement() * b.element_size() for b in model.buffers())
    return {
        "labels": list(model.labels) if hasattr(model, "labels") else None,
        "parameters": int(sum(p.numel() for p in model.parameters())),
        "trainable_parameters": int(sum(p.numel() for p in model.parameters() if p.requires_grad)),
        "memory_mib": float((param_bytes + buffer_bytes) / 1024**2),
        "checkpoint_path": str(checkpoint),
        "checkpoint_mib": float(checkpoint.stat().st_size / 1024**2),
    }


def hardware_info(device_mode: str) -> dict[str, Any]:
    info = {
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "torch_version": getattr(torch, "__version__", "unknown"),
        "cuda_available": bool(torch.cuda.is_available()),
    }

    if device_mode == "cpu":
        info.update(
            {
                "cpu_count": int(torch.get_num_threads()),
                "torch_num_threads": int(torch.get_num_threads()),
                "torch_num_interop_threads": int(torch.get_num_interop_threads()),
            }
        )
        return info

    info.update(
        {
            "cuda_version": getattr(torch.version, "cuda", None),
            "device_count": int(torch.cuda.device_count()) if torch.cuda.is_available() else 0,
        }
    )
    if torch.cuda.is_available():
        props = torch.cuda.get_device_properties(0)
        info["gpu_name"] = props.name
        info["gpu_total_mem_gb"] = round(props.total_memory / 1024**3, 2)
    return info


@torch.inference_mode()
def preload_batches(
    model: torch.nn.Module,
    dataloader: DataLoader,
    device: torch.device,
    max_batches: int,
) -> tuple[list[torch.Tensor], dict[str, Any]]:
    batches: list[torch.Tensor] = []
    n_samples = 0
    first_batch_shape = None
    start = time.perf_counter()

    model.to(device)
    model.eval()
    for i, batch in enumerate(dataloader):
        if i >= max_batches:
            break
        x = batch["X"].to(device, non_blocking=(device.type == "cuda"))
        if hasattr(model, "annotate_batch_pre"):
            x = model.annotate_batch_pre(x, {})
        batches.append(x)
        n_samples += int(x.size(0))
        if first_batch_shape is None:
            first_batch_shape = list(x.shape)

    if device.type == "cuda":
        torch.cuda.synchronize()
    return batches, {
        "batches": int(len(batches)),
        "samples": int(n_samples),
        "first_batch_shape": first_batch_shape,
        "seconds_excluded_from_timing": float(time.perf_counter() - start),
        "preloaded_to_device": str(device),
        "preprocessed_before_timing": True,
    }


@torch.inference_mode()
def time_model_only_once(
    model: torch.nn.Module,
    batches: list[torch.Tensor],
    device: torch.device,
    warmup: int,
) -> dict[str, Any]:
    if not batches:
        raise ValueError("No batches were preloaded; cannot measure throughput.")

    n_batches = len(batches)
    n_samples = sum(int(x.size(0)) for x in batches)

    for i in range(warmup):
        _ = model(batches[i % n_batches])
    if device.type == "cuda":
        torch.cuda.synchronize()

    if device.type == "cuda":
        start = torch.cuda.Event(enable_timing=True)
        end = torch.cuda.Event(enable_timing=True)
        start.record()
        for x in batches:
            _ = model(x)
        end.record()
        end.synchronize()
        seconds = max(start.elapsed_time(end) / 1000.0, 1e-9)
        timer = "cuda_event"
    else:
        start = time.perf_counter()
        for x in batches:
            _ = model(x)
        seconds = max(time.perf_counter() - start, 1e-9)
        timer = "perf_counter"

    return {
        "samples_per_sec": float(n_samples / seconds),
        "batches": int(n_batches),
        "samples": int(n_samples),
        "seconds": float(seconds),
        "timer": timer,
    }


def summarize_runs(runs: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(run["samples_per_sec"]) for run in runs]
    return {
        "mean_samples_per_sec": float(statistics.fmean(values)),
        "std_samples_per_sec": float(statistics.pstdev(values)) if len(values) > 1 else 0.0,
        "min_samples_per_sec": float(min(values)),
        "max_samples_per_sec": float(max(values)),
        "n_runs": int(len(values)),
        "runs": runs,
    }


def run_benchmark(
    *,
    script_path: Path,
    model_name: str,
    device_mode: str,
    default_checkpoint: Path,
    default_out: Path,
    description: str,
    build_model: Callable[[Path], torch.nn.Module],
) -> None:
    args = parse_args(
        description=description,
        default_checkpoint=default_checkpoint,
        default_out=default_out,
        device_mode=device_mode,
    )
    if args.max_batches <= 0:
        raise ValueError("--max-batches must be positive.")
    if args.runs <= 0:
        raise ValueError("--runs must be positive.")

    if device_mode == "cpu":
        configure_torch_threads(args)
        device = torch.device("cpu")
    else:
        if args.device == "cuda" and not torch.cuda.is_available():
            raise RuntimeError("CUDA was requested but torch.cuda.is_available() is false.")
        device = torch.device(args.device)

    np.random.seed(args.seed)
    torch.manual_seed(args.seed)

    model = build_model(args.checkpoint)
    dataloader = build_eval_loader(
        outroot=args.outroot,
        years=args.years,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
        include_labels=args.include_labels,
        model_labels=model.labels,
        pin_memory=(device_mode != "cpu"),
    )

    label = f"{model_name} cpu" if device_mode == "cpu" else model_name
    print(f"[{label}] preloading {args.max_batches} batches to {device} ...")
    batches, preload = preload_batches(model, dataloader, device, args.max_batches)
    print(
        f"[{label}] timing model(x) only: "
        f"{preload['samples']} samples across {preload['batches']} batches, {args.runs} runs"
    )
    runs = [time_model_only_once(model, batches, device, args.warmup) for _ in range(args.runs)]
    throughput = summarize_runs(runs)
    print(
        f"[{label}] {throughput['mean_samples_per_sec']:.2f} +/- "
        f"{throughput['std_samples_per_sec']:.2f} samples/s"
    )

    if device_mode == "cpu":
        metric = "model_only_cpu_inference_throughput"
        throughput_key = "throughput_model_only_cpu"
        excluded = ["dataloader iteration", "annotate_batch_pre"]
        device_config = "cpu"
    else:
        metric = "model_only_inference_throughput"
        throughput_key = "throughput_model_only"
        excluded = ["dataloader iteration", "host-to-device transfer", "annotate_batch_pre"]
        device_config = args.device

    config = {
        "outroot": str(args.outroot),
        "years": args.years,
        "batch_size": args.batch_size,
        "num_workers": args.num_workers,
        "max_batches": args.max_batches,
        "warmup": args.warmup,
        "runs": args.runs,
        "seed": args.seed,
        "device": device_config,
        "include_labels": args.include_labels,
    }
    if device_mode == "cpu":
        config["torch_threads"] = args.torch_threads
        config["torch_interop_threads"] = args.torch_interop_threads

    payload = {
        "script": str(script_path.resolve()),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model": model_name,
        "metric": metric,
        "timed_region": "model(x)",
        "excluded_from_timing": excluded,
        "config": config,
        "hardware": hardware_info(device_mode),
        "model_info": model_info(model, args.checkpoint),
        "preload": preload,
        throughput_key: throughput,
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(f"[json] wrote {args.out}")
