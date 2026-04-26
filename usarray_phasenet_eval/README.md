# Quick Start

The material in this directory has two intended uses:

1. Paper reproduction: the data processing, training, and evaluation programs
   used for the paper are kept in the corresponding subdirectories.
2. Quick start and playground: `sample_eval.py` provides a small, runnable
   evaluation workflow for trying the contributed USArray PhaseNet model.


## Sample Eval
This directory contains a small USArray sample dataset, a USArray-trained
PhaseNet checkpoint, and `sample_eval.py`, which compares:

- `PhaseNet-NCEDC`: SeisBench's pretrained PhaseNet model named `original`
- `PhaseNet-USArray`: the checkpoint in `checkpoint/phasenet-usarray.pt`


### Create a Python Environment

From the repository root:

```bash
cd usarray_phasenet_eval
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Run the Sample Evaluation

Run with the default sample dataset and checkpoint:

```bash
python sample_eval.py
```

This writes:

```text
phasenet_comparison_metrics.json
```

### Notes

`sample_eval.py` defaults to:

```text
--dataset usarray_phasenet_eval/usarray_samples
--usarray-checkpoint usarray_phasenet_eval/checkpoint/phasenet-usarray.pt
--pretrained-name original
```

On the first run, SeisBench may download the pretrained `original` model (PhaseNet-NCEDC) into
its default cache location. Subsequent runs should reuse the cached model.
