"""Synthetic 64 MiB completion comparison; run each mode in a fresh process.

Usage: python benchmark_completion_memory.py legacy|fixed
No AWS, MongoDB, or Dask calls. RSS is sampled during pickle writes, so shorter
peaks may be missed. This is not a GeoLab benchmark or a bound on real day sizes.
"""

import json
import pickle
import sys
from contextlib import contextmanager

import psutil
from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble

import s3_day_workflow as workflow

process = psutil.Process()
baseline = process.memory_info().rss
peak = baseline
written = 0


class Sink:
    def write(self, data):
        global peak, written
        count = memoryview(data).nbytes
        written += count
        peak = max(peak, process.memory_info().rss)
        return count


@contextmanager
def open_sink(key, mode):
    yield Sink()


def legacy_save(reader_output):
    merged = TimeSeriesEnsemble()
    for ensemble in reader_output:
        for trace in ensemble.member:
            merged.member.append(trace)
    merged["jday_tag"] = "2014_1"
    merged.set_live()
    del reader_output
    pickle.dump(merged, Sink())
    return True


def build():
    outputs = []
    for _ in range(32):
        ensemble = TimeSeriesEnsemble(16)
        for _ in range(16):
            trace = TimeSeries(16384)
            trace.set_live()
            ensemble.member.append(trace)
        ensemble["jday_tag"] = "2014_1"
        ensemble.set_live()
        outputs.append(ensemble)
    return outputs


result = build()
mode = sys.argv[1]
if mode not in ("legacy", "fixed"):
    raise ValueError("mode must be legacy or fixed")
if mode == "legacy":
    result = legacy_save(result)
else:
    workflow.fetch_dbhandle = lambda handle: {}
    result = workflow.save_jday_outputs(
        result,
        None,
        type("FS", (), {"open": staticmethod(open_sink)})(),
        "s3://test/u",
        2014,
    )
assert result is True
print(
    json.dumps(
        {
            "mode": mode,
            "baseline_mib": baseline / 2**20,
            "observed_peak_mib": peak / 2**20,
            "peak_over_baseline_mib": (peak - baseline) / 2**20,
            "pickle_mib": written / 2**20,
        },
        sort_keys=True,
    )
)
