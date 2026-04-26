#!/usr/bin/env python3
"""Convert MsPASS TimeSeries records into SeisBench waveform datasets."""

from __future__ import annotations

import argparse
import os
import random
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
from mspasspy.db.client import DBClient
from mspasspy.util.converter import TimeSeries2Trace
from seisbench.data.base import GeometricBucketer, WaveformDataWriter


DEFAULT_YEARS = ("2007", "2008", "2011", "2012", "2013")
TRAIN_DEV_TEST = (0.8, 0.1, 0.1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build SeisBench datasets from MsPASS-indexed USArray waveforms."
    )
    parser.add_argument(
        "--years",
        nargs="+",
        default=list(DEFAULT_YEARS),
        help="Years to export. Defaults to the years used by the original workflow.",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path("./seisbench_usarray"),
        help="Output directory for generated SeisBench datasets.",
    )
    parser.add_argument(
        "--mongo-host",
        default=os.environ.get("USARRAY_MONGO_HOST", "localhost"),
        help="MongoDB host. Can also be set with USARRAY_MONGO_HOST.",
    )
    parser.add_argument("--mongo-port", type=int, default=27020)
    parser.add_argument("--mongo-db-prefix", default="usarray")
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--sampling-rate", type=int, default=40)
    parser.add_argument("--freqmin", type=float, default=0.5)
    parser.add_argument("--freqmax", type=float, default=5.0)
    parser.add_argument("--filter-corners", type=int, default=5)
    parser.add_argument("--no-filter-zerophase", action="store_true")
    return parser.parse_args()


def writer_data_format(sampling_rate: int) -> dict[str, object]:
    return {
        "dimension_order": "CW",
        "component_order": "Z",
        "sampling_rate": sampling_rate,
    }


def build_trace_metadata(trace, doc: dict, sampling_rate: int) -> tuple[dict, np.ndarray]:
    waveform = trace.data.astype("float32")[None, :]
    metadata = {
        "trace_name": f"{trace.id}_{uuid.uuid4().hex[:8]}",
        "trace_start_time": trace.stats.starttime.isoformat(),
        "trace_sampling_rate_hz": trace.stats.sampling_rate,
        "trace_npts": trace.stats.npts,
        "trace_channel": trace.stats.channel[:2],
        "split": random.choices(["train", "dev", "test"], TRAIN_DEV_TEST)[0],
        "trace_P_arrival_sample": np.nan,
    }

    for pick_time in doc["manual_picks"]:
        if not isinstance(pick_time, (float, int)):
            raise TypeError(f"Expected numeric pick time, received {type(pick_time)!r}")

        sample = round((pick_time - trace.stats.starttime.timestamp) * sampling_rate)
        if 0 <= sample < trace.stats.npts:
            metadata["trace_P_arrival_sample"] = sample
            break

    return metadata, waveform


def process_doc(db, doc: dict, args: argparse.Namespace) -> tuple[dict, np.ndarray]:
    timeseries = db.read_data(doc)
    trace = TimeSeries2Trace(timeseries)

    if int(trace.stats.sampling_rate) != args.sampling_rate:
        trace = trace.resample(args.sampling_rate)
        if int(trace.stats.sampling_rate) != args.sampling_rate:
            raise ValueError(
                f"Resampling failed: expected {args.sampling_rate}, "
                f"got {trace.stats.sampling_rate}"
            )

    trace = trace.filter(
        "bandpass",
        freqmin=args.freqmin,
        freqmax=args.freqmax,
        corners=args.filter_corners,
        zerophase=not args.no_filter_zerophase,
    )

    return build_trace_metadata(trace, doc, args.sampling_rate)


def build_year_dataset(dbclient: DBClient, year: str, args: argparse.Namespace) -> None:
    print(f"Starting dataset export for year {year}")

    outdir = args.outdir / year
    outdir.mkdir(parents=True, exist_ok=True)

    metadata_file = outdir / f"metadata_{year}.csv"
    waveform_file = outdir / f"waveforms_{year}.hdf5"

    mongo_db_name = f"{args.mongo_db_prefix}{year}"
    db = dbclient.get_database(mongo_db_name)
    query = {
        "manual_picks.0": {"$exists": True},
        "manual_picks.1": {"$exists": False},
        "chan": "BHZ",
    }

    total_docs = db.wf_miniseed.count_documents(query)
    print(f"Total docs with exactly one manual pick: {total_docs:,}")

    cursor = db.wf_miniseed.find(query, batch_size=args.batch_size)
    write_lock = threading.Lock()

    with WaveformDataWriter(metadata_file, waveform_file) as writer, ThreadPoolExecutor(
        max_workers=args.max_workers
    ) as pool:
        writer.data_format = writer_data_format(args.sampling_rate)
        writer.bucket_size = 4096
        writer.bucketer = GeometricBucketer(minbucket=2000, factor=1.1, splits=True)

        jobs = pool.map(lambda doc: process_doc(db, doc, args), cursor, chunksize=16)
        for index, (metadata, waveform) in enumerate(jobs, 1):
            with write_lock:
                writer.add_trace(metadata, waveform)

            if index % 1_000 == 0:
                print(f"Processed {index:,} docs")

        writer.flush_hdf5()

    print(f"Finished year {year}; dataset stored in {outdir}")


def main() -> None:
    args = parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)

    dbclient = DBClient(f"{args.mongo_host}:{args.mongo_port}")
    for year in args.years:
        build_year_dataset(dbclient, year, args)


if __name__ == "__main__":
    main()
