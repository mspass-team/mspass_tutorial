#!/usr/bin/env python3
# coding: utf-8
"""
High-utilization PhaseNet trainer for A100
- Pinned-memory collate so async H2D works
- CUDA prefetcher overlaps H2D copies with compute
- bf16 autocast on Ampere (fallback to fp16)
- TF32 enabled; cudnn benchmark on
- torch.compile for fused, faster kernels (PyTorch 2+)
- Safe with HDF5 via spawn + __main__ guard

Env knobs (optional):
  BATCH=2048 EPOCHS=50 LR=1e-4 PATIENCE=8 TORCH_COMPILE_MODE=max-autotune
  NWORKERS_TRAIN=12 PREFETCH_TRAIN=8 NWORKERS_DEV=4 PREFETCH_DEV=4
  USARRAY_SEISBENCH_ROOT=/path/to/seisbench_usarray
"""

import os
os.environ["TORCHINDUCTOR_CUDAGRAPHS"] = "0"
import argparse
import json
import time
from pathlib import Path
from typing import Tuple, Dict, Any

import numpy as np
import torch
from torch import nn

def disable_inplace(m: nn.Module):
    for mod in m.modules():
        if hasattr(mod, "inplace") and getattr(mod, "inplace"):
            mod.inplace = False

from torch.utils.data import DataLoader
import torch.multiprocessing as mp
mp.set_sharing_strategy("file_descriptor")

import seisbench.data as sbd
import seisbench.generate as sbg
import seisbench.models as sbm
from seisbench.util import worker_seeding

# Keep the original artifact filename used in the HPC experiments. In this repo,
# this checkpoint corresponds to the PhaseNet-USArray model.
ckpt_fname = f'new_phasenet_gpu_original.pt'

# ---------------------------------------------------------------------
# Global environment safety for HDF5 (can be overridden by env)
# ---------------------------------------------------------------------
# os.environ.setdefault("HDF5_USE_FILE_LOCKING", "FALSE")
# os.environ.setdefault("HDF5_DRIVER", "core")
# os.environ.setdefault("HDF5_METADATA_CACHE_SIZE", "1048576")
# os.environ.setdefault("OMP_NUM_THREADS", "1")


# ---------------------------------------------------------------------
# Collate: convert NumPy → CPU torch so pin_memory thread can pin them
# ---------------------------------------------------------------------
def collate_to_torch(batch: list[Dict[str, Any]]) -> Dict[str, torch.Tensor]:
    X = np.stack([b["X"] for b in batch], axis=0)  # (B, C, W)
    y = np.stack([b["y"] for b in batch], axis=0)  # (B, L, W)
    return {"X": torch.from_numpy(X), "y": torch.from_numpy(y)}


# ---------------------------------------------------------------------
# CUDA Prefetcher (overlap H2D with compute)
# ---------------------------------------------------------------------
class CudaPrefetcher:
    def __init__(self, loader: DataLoader, device: torch.device, target_dtype: torch.dtype | None = None):
        self._iter = iter(loader)
        self.device = device
        self.target_dtype = target_dtype
        self.stream = torch.cuda.Stream() if device.type == "cuda" else None
        self._next = None
        self._preload()

    def _preload(self):
        try:
            batch = next(self._iter)
        except StopIteration:
            self._next = None
            return

        def move(t: torch.Tensor) -> torch.Tensor:
            # Only pin when moving to CUDA and tensor is CPU and not already pinned
            if self.device.type == "cuda" and t.device.type == "cpu" and not t.is_pinned():
                t = t.pin_memory()
            # Non-blocking H2D
            t = t.to(self.device, non_blocking=True)
            # Avoid dtype casts here; rely on autocast during compute
            return t

        if self.stream is None:
            x = move(batch["X"])
            y = move(batch["y"])
            self._next = (x, y)
            return

        with torch.cuda.stream(self.stream):
            x = move(batch["X"])
            y = move(batch["y"])
            self._next = (x, y)

    def __iter__(self):
        return self

    def __next__(self):
        if self._next is None:
            raise StopIteration
        if self.stream is not None:
            torch.cuda.current_stream().wait_stream(self.stream)
        out = self._next
        self._preload()
        return out


# ---------------------------------------------------------------------
# Loss
# ---------------------------------------------------------------------
def loss_fn(y_pred: torch.Tensor, y_true: torch.Tensor, eps: float = 1e-5) -> torch.Tensor:
    # If model outputs logits, switch to log_softmax here. Assuming probs, clamp to avoid log(0).
    yp = y_pred.float().clamp_min(eps)
    yt = y_true.float()
    h = yt * torch.log(yp)
    h = h.mean(-1).sum(-1)
    return -h.mean()



# ---------------------------------------------------------------------
# Train / Eval
# ---------------------------------------------------------------------
def train_one_epoch(
    dataloader: DataLoader,
    model: nn.Module,
    optimizer: torch.optim.Optimizer,
    device: torch.device,
    amp_dtype: torch.dtype,
    scaler: torch.cuda.amp.GradScaler,
    use_amp: bool,
    epoch: int,
    profile_steps: int = 0,
) -> float:
    t0 = time.time()
    model.train()
    total, seen = 0.0, 0
    prefetch = CudaPrefetcher(dataloader, device, target_dtype=None)
    # Lightweight per-step profiler (first N steps only)
    prof_n = profile_steps if epoch == 1 else 0
    prev_end = time.perf_counter()
    prof = {"data": 0.0, "fwd": 0.0, "bwd": 0.0, "step": 0.0, "iters": 0, "samples": 0}

    step = 0
    for step, (x, y) in enumerate(prefetch, start=1):
        if prof_n and step <= prof_n:
            t_iter_start = time.perf_counter()
            prof["data"] += t_iter_start - prev_end
        x = model.annotate_batch_pre(x, {})  # keeps tensor on device
        optimizer.zero_grad(set_to_none=True)

        if use_amp and device.type == "cuda":
            t_fwd_start = time.perf_counter() if prof_n and step <= prof_n else None
            with torch.autocast("cuda", dtype=amp_dtype):
                pred = model(x)
                loss = loss_fn(pred, y)
        else:
            t_fwd_start = time.perf_counter() if prof_n and step <= prof_n else None
            pred = model(x)
            loss = loss_fn(pred, y)
        if prof_n and step <= prof_n:
            prof["fwd"] += time.perf_counter() - t_fwd_start

        t_bwd_start = time.perf_counter() if prof_n and step <= prof_n else None
        if scaler and scaler.is_enabled():
            scaler.scale(loss).backward()
            scaler.step(optimizer)
            scaler.update()
        else:
            loss.backward()
            optimizer.step()
        if prof_n and step <= prof_n:
            prof["bwd"] += time.perf_counter() - t_bwd_start

        bsz = x.size(0)
        total += loss.item() * bsz
        seen += bsz
        if prof_n and step <= prof_n:
            now = time.perf_counter()
            prof["step"] += now - t_iter_start
            prof["iters"] += 1
            prof["samples"] += bsz
            prev_end = now

    t1 = time.time()
    print(f"  epoch {epoch} train time: {t1 - t0:.2f}s  avg_step: {(t1 - t0)/max(1, step):.4f}s")
    if prof["iters"]:
        it = prof["iters"]
        print(
            "    profile(first %d): data=%.4fs fwd=%.4fs bwd=%.4fs step=%.4fs samples/s=%.1f"
            % (
                it,
                prof["data"] / it,
                prof["fwd"] / it,
                prof["bwd"] / it,
                prof["step"] / it,
                prof["samples"] / (prof["step"] if prof["step"] > 0 else 1e-6),
            )
        )
    return total / max(1, seen)


def eval_one_epoch(
    dataloader: DataLoader,
    model: nn.Module,
    device: torch.device,
    amp_dtype: torch.dtype,
    use_amp: bool,
    epoch: int,
) -> float:
    model.eval()
    total, seen = 0.0, 0
    prefetch = CudaPrefetcher(dataloader, device, target_dtype=None)

    with torch.inference_mode():
        for step, (x, y) in enumerate(prefetch, start=1):
            x = model.annotate_batch_pre(x, {})
            if use_amp and device.type == "cuda":
                with torch.autocast("cuda", dtype=amp_dtype):
                    pred = model(x)
                    loss = loss_fn(pred, y)
            else:
                pred = model(x)
                loss = loss_fn(pred, y)
            bsz = x.size(0)
            total += loss.item() * bsz
            seen += bsz

    return total / max(1, seen)


def train_loop(
    model: nn.Module,
    train_loader: DataLoader,
    dev_loader: DataLoader,
    device: torch.device,
    amp_dtype: torch.dtype,
    scaler: torch.cuda.amp.GradScaler,
    use_amp: bool,
    *,
    epochs: int = 50,
    learning_rate: float = 1e-4,
    patience: int = 5,
    min_delta: float = 0.0,
    val_interval: int = 5,
    profile_steps: int = 0,
    ckpt_path: str | Path = "phasenet_best.pt",
    history_path: str | Path = "phasenet_loss_history.json",
) -> Dict[str, Any]:
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)
    best_val, best_epoch, wait = float("inf"), -1, 0
    hist_train, hist_val = [], []

    for epoch in range(1, epochs + 1):
        t0 = time.time()
        tr = train_one_epoch(train_loader, model, optimizer, device, amp_dtype, scaler, use_amp, epoch, profile_steps)
        # Evaluate every val_interval epochs and on the final epoch
        do_eval = (val_interval is None) or (val_interval <= 1) or (epoch % max(1, val_interval) == 0) or (epoch == epochs)
        val = eval_one_epoch(dev_loader, model, device, amp_dtype, use_amp, epoch) if do_eval else None
        t1 = time.time()

        hist_train.append(tr)
        hist_val.append(val)
        if do_eval:
            print(f"[{epoch:3d}/{epochs}]  train={tr:.6f}  val={val:.6f}  epoch_time={t1 - t0:.2f}s")
        else:
            print(f"[{epoch:3d}/{epochs}]  train={tr:.6f}  val=skipped  epoch_time={t1 - t0:.2f}s")

        if do_eval:
            if val < best_val - min_delta:
                best_val, best_epoch, wait = val, epoch, 0
                to_save = model
                if hasattr(model, "_orig_mod"):  # compiled model
                    to_save = model._orig_mod
                    
                torch.save(to_save.state_dict(), ckpt_path)
                print(f"  ↳ New best model saved to '{ckpt_path}' (val={best_val:.6f})")
            else:
                wait += 1
                if wait >= patience:
                    print(f"  ↳ Early stopping (no improvement for {patience} evaluated epochs).")
                    break

    with open(history_path, "w") as fp:
        json.dump({"train": hist_train, "val": hist_val, "best_epoch": best_epoch}, fp)
    print(f"Loss history written to '{history_path}'.  Best epoch: {best_epoch} (val {best_val:.6f})")
    return {"train": hist_train, "val": hist_val, "best_epoch": best_epoch}


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
def bf16_supported() -> bool:
    if not torch.cuda.is_available():
        return False
    major, _ = torch.cuda.get_device_capability()
    return major >= 8  # Ampere+


def build_generators(train, dev, labels) -> Tuple[sbg.GenericGenerator, sbg.GenericGenerator]:
    phase_dict = {
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
    train_gen = sbg.GenericGenerator(train)
    dev_gen   = sbg.GenericGenerator(dev)

    augmentations = [
        sbg.WindowAroundSample(list(phase_dict.keys()), samples_before=3000, windowlen=6000,
                               selection="random", strategy="variable"),
        sbg.RandomWindow(windowlen=3001, strategy="pad"),
        sbg.ChangeDtype(np.float32),
        sbg.ProbabilisticLabeller(label_columns=phase_dict, model_labels=labels, sigma=30, dim=0),
    ]
    train_gen.add_augmentations(augmentations)
    dev_gen.add_augmentations(augmentations)
    return train_gen, dev_gen


def build_loaders(train_gen, dev_gen) -> Tuple[DataLoader, DataLoader]:
    train_workers = int(os.getenv("NWORKERS_TRAIN", 4))
    dev_workers   = int(os.getenv("NWORKERS_DEV",   2))
    batch_size    = int(os.getenv("BATCH", "512"))
    train_pref    = int(os.getenv("PREFETCH_TRAIN", "2"))
    dev_pref      = int(os.getenv("PREFETCH_DEV", "2"))
    pin           = torch.cuda.is_available()

    train_kwargs = dict(
        dataset=train_gen,
        batch_size=batch_size,
        shuffle=True,
        num_workers=train_workers,
        pin_memory=pin,
        persistent_workers=(train_workers > 0),
        worker_init_fn=worker_seeding,
        collate_fn=collate_to_torch,
        drop_last=True,
        timeout=0,
        multiprocessing_context="spawn"
    )
    if train_workers > 0:
        train_kwargs["prefetch_factor"] = train_pref
    train_loader = DataLoader(**train_kwargs)

    dev_kwargs = dict(
        dataset=dev_gen,
        batch_size=batch_size,
        shuffle=False,
        num_workers=dev_workers,
        pin_memory=pin,
        persistent_workers=(dev_workers > 0),
        worker_init_fn=worker_seeding,
        collate_fn=collate_to_torch,
        drop_last=False,
        timeout=0,
        multiprocessing_context="spawn",
    )
    if dev_workers > 0:
        dev_kwargs["prefetch_factor"] = dev_pref
    dev_loader = DataLoader(**dev_kwargs)
    print(f"train_loader: workers={train_workers}, prefetch_factor={train_pref}, batch_size={batch_size}")
    print(f"dev_loader:   workers={dev_workers},   prefetch_factor={dev_pref},   batch_size={batch_size}")
    return train_loader, dev_loader

# Device & backend knobs
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# Decide one dtype for the whole run (activations under autocast; keep weights FP32)
TARGET_DTYPE = torch.bfloat16 if (device.type == "cuda" and bf16_supported()) else torch.float32

def parse_args():
    p = argparse.ArgumentParser(description="PhaseNet-USArray training entrypoint")
    p.add_argument("--year",
                   type=str,
                   default="all",
                   help="Which year of data to use (e.g., 2007, 2008, ...). Use 'all' to include all years.")
    p.add_argument(
        "--outroot",
        type=Path,
        default=Path(os.getenv("USARRAY_SEISBENCH_ROOT", "./seisbench_usarray")),
        help="Root directory containing SeisBench year datasets. "
        "Defaults to USARRAY_SEISBENCH_ROOT or ./seisbench_usarray.",
    )
    p.add_argument("--use-pretrained",
                   action="store_true",
                   help="If set, initialize the model from a saved checkpoint.")
    return p.parse_args()

def main():
    args = parse_args()

    # Safer with h5py/HDF5 + PyTorch workers
    try:
        torch.multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        raise

    if device.type == "cuda":
        torch.backends.cudnn.benchmark = True
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
        torch.set_float32_matmul_precision("high")
        print("Using CUDA:", torch.cuda.get_device_name(0))
    else:
        print("Using CPU")

    print("TARGET_DTYPE:", TARGET_DTYPE)
    amp_dtype = TARGET_DTYPE
    use_amp = (device.type == "cuda" and amp_dtype == torch.bfloat16)
    scaler = torch.cuda.amp.GradScaler(enabled=False)

    # ---------------------------
    # Datasets (single year or all)
    # ---------------------------
    ALL_YEARS = ["2007", "2008", "2011", "2012", "2013"]
    outroot = args.outroot

    if args.year.lower() == "all":
        years = ALL_YEARS
        year_label = "all"
    else:
        y = str(args.year)
        if y not in ALL_YEARS:
            print(f"[warn] Requested year {y} not in default list {ALL_YEARS}. "
                  "Proceeding anyway (make sure the path exists).")
        years = [y]
        year_label = y

    print("Using year(s):", years)
    print("Using dataset root:", outroot)
    datasets = [sbd.WaveformDataset(outroot / f"{yr}") for yr in years]
    ds = sbd.MultiWaveformDataset(datasets)
    train, dev, test = ds.train_dev_test()
    print("Train size:", len(train), "Dev size:", len(dev), "Test size:", len(test))

    # ---------------------------
    # Model
    # ---------------------------
    model = sbm.PhaseNet(phases="PSN", norm="std", default_args={"blinding": (200, 200)}).to(device)
    disable_inplace(model)

    # Optionally load pretrained weights BEFORE torch.compile
    initialized_from = "scratch"
    if args.use_pretrained:
        ckpt_path = Path(ckpt_fname)
        if not ckpt_path.exists():
            print(f"[warn] --use-pretrained set but checkpoint not found at {ckpt_path}. Training from scratch.")
        else:
            print(f"Loading pretrained weights from: {ckpt_path}")
            try:
                checkpoint = torch.load(ckpt_path, map_location=device)
                if "state_dict" in checkpoint:
                    state = checkpoint["state_dict"]
                else:
                    state = checkpoint
    
                # If the ckpt was saved from a compiled model, strip "_orig_mod."
                fixed_state = {}
                for k, v in state.items():
                    fixed_state[k.replace("_orig_mod.", "")] = v
                
                incompatible = model.load_state_dict(fixed_state, strict=False)
                missing = incompatible.missing_keys
                unexpected = incompatible.unexpected_keys
    
                if missing:
                    print(f"[info] Missing keys ({len(missing)}): {missing[:8]}{' ...' if len(missing) > 8 else ''}")
                if unexpected:
                    print(f"[info] Unexpected keys ({len(unexpected)}): {unexpected[:8]}{' ...' if len(unexpected) > 8 else ''}")
    
                initialized_from = str(ckpt_path)
            except Exception as e:
                print(f"[warn] Failed to load checkpoint: {e}. Proceeding from scratch.")
    
    # Optional: torch.compile (compile AFTER potential load so dtype/graph are captured)
    if hasattr(torch, "compile"):
        backend = os.getenv("TORCH_COMPILE_BACKEND", "inductor")
        mode    = os.getenv("TORCH_COMPILE_MODE", "max-autotune")
        model = torch.compile(model, backend=backend, mode=mode, dynamic=True)
        print(f"Compiled model with torch.compile(backend='{backend}', mode='{mode}')")

    # ---------------------------
    # Generators & loaders
    # ---------------------------
    train_gen, dev_gen = build_generators(train, dev, labels=model.labels)
    train_loader, dev_loader = build_loaders(train_gen, dev_gen)

    # ---------------------------
    # Train
    # ---------------------------
    if args.use_pretrained and initialized_from != "scratch":
        print(f"Starting training from pretrained checkpoint: {initialized_from}")
    else:
        print("Starting training from scratch...")

    history_fname = f"phasenet_loss_history_{year_label}.json"

    hist = train_loop(
        model,
        train_loader,
        dev_loader,
        device,
        amp_dtype,
        scaler,
        use_amp,
        epochs=int(os.getenv("EPOCHS", "50")),
        learning_rate=float(os.getenv("LR", "1e-4")),
        patience=int(os.getenv("PATIENCE", "2")),
        val_interval=int(os.getenv("EVAL_INTERVAL", "5")),
        profile_steps=int(os.getenv("PROFILE_STEPS", "10")),
        ckpt_path=os.getenv("CKPT", ckpt_fname),
        history_path=history_fname,
    )

    # ---------------------------
    # Post-run summary
    # ---------------------------
    ckpt = Path(os.getenv("CKPT", ckpt_fname))
    try:
        best = sbm.PhaseNet(phases="PSN", norm="std", default_args={"blinding": (200, 200)})
        state = torch.load(ckpt, map_location="cpu")
        best.load_state_dict(state, strict=False)
        total_params = sum(p.numel() for p in best.parameters())
        trainable_params = sum(p.numel() for p in best.parameters() if p.requires_grad)
        param_bytes = sum(p.nelement() * p.element_size() for p in best.parameters())
        buffer_bytes = sum(b.nelement() * b.element_size() for b in best.buffers())
        total_bytes = param_bytes + buffer_bytes
        print("Best model summary:")
        print(f" - Best epoch: {hist.get('best_epoch') if isinstance(hist, dict) else 'n/a'}")
        print(f" - Parameters: {total_params:,} (trainable: {trainable_params:,})")
        print(
            f" - In-memory size: {total_bytes/1024**2:.2f} MiB "
            f"(params: {param_bytes/1024**2:.2f} MiB, buffers: {buffer_bytes/1024**2:.2f} MiB)"
        )
        if ckpt.exists():
            print(f" - Checkpoint size: {ckpt.stat().st_size/1024**2:.2f} MiB at {ckpt}")
    except Exception as e:
        print("Post-run summary failed:", e)


if __name__ == "__main__":
    main()
