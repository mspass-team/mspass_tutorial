#!/usr/bin/env python3
"""Extract and index USArray miniSEED archives into an MsPASS database."""

from __future__ import annotations

import argparse
import os
import tarfile
from pathlib import Path
from time import time

from bson.codec_options import CodecOptions
from mspasspy.db.client import DBClient


def section(title: str, width: int = 80, char: str = "#") -> None:
    """Print a compact section banner for long HPC logs."""
    if width < len(title) + 4:
        width = len(title) + 4
    pad = width - len(title) - 2
    left, right = pad // 2, pad - pad // 2
    bar = char * width
    print(bar, f"{char}{' ' * left}{title}{' ' * right}{char}", bar, sep="\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract USArray miniSEED archives and index them in MongoDB."
    )
    parser.add_argument("year", type=int, help="Four-digit year, for example 2015")
    parser.add_argument(
        "--scratch-root",
        type=Path,
        default=os.environ.get("USARRAY_SCRATCH_ROOT"),
        required="USARRAY_SCRATCH_ROOT" not in os.environ,
        help="Base scratch directory. Can also be set with USARRAY_SCRATCH_ROOT.",
    )
    parser.add_argument(
        "--mongo-host",
        default=os.environ.get("USARRAY_MONGO_HOST", "localhost"),
        help="MongoDB host. Can also be set with USARRAY_MONGO_HOST.",
    )
    parser.add_argument("--mongo-port", type=int, default=27020)
    parser.add_argument("--mongo-db-prefix", default="usarray")
    return parser.parse_args()


def extract_mseed_archives(data_dir: Path, extract_root: Path) -> list[str]:
    """Extract all miniSEED members from tar archives and return their paths."""
    tar_files = sorted(data_dir.glob("*.tar"))
    if not tar_files:
        raise SystemExit(f"No .tar archives found under {data_dir}")

    extract_root.mkdir(parents=True, exist_ok=True)
    mseed_paths: list[str] = []

    for tar_path in tar_files:
        with tarfile.open(tar_path, mode="r:*") as tar:
            members = [member for member in tar.getmembers() if member.name.endswith(".mseed")]
            tar.extractall(path=extract_root, members=members)
            mseed_paths.extend(str(extract_root / member.name) for member in members)

    return mseed_paths


def main() -> None:
    args = parse_args()
    mongo_db_name = f"{args.mongo_db_prefix}{args.year}"
    data_dir = args.scratch_root / f"usarray_{args.year}" / str(args.year) / "wf"
    extract_root = args.scratch_root / f"usarray_{args.year}" / str(args.year) / "extracted"

    print(f"Indexing year={args.year} data_dir={data_dir}")
    print(f"MongoDB target: {args.mongo_host}:{args.mongo_port}/{mongo_db_name}")

    section("1. Connect to MongoDB")
    opts = CodecOptions(tz_aware=True)
    db = DBClient(f"{args.mongo_host}:{args.mongo_port}").get_database(
        mongo_db_name,
        codec_options=opts,
    )

    existing_docs = db.wf_miniseed.estimated_document_count()
    if existing_docs:
        result = db.wf_miniseed.delete_many({})
        print(f"Removed {result.deleted_count:,} existing wf_miniseed documents")

    section("2. Extract miniSEED archives")
    mseed_paths = extract_mseed_archives(data_dir, extract_root)
    print(f"Located {len(mseed_paths):,} miniSEED files after extraction")

    section("3. Index miniSEED files")
    total = 0.0
    for index, fpath in enumerate(mseed_paths, 1):
        start = time()
        db.index_mseed_file(fpath, Path(fpath).parent)
        total += time() - start
        if index % 10 == 0:
            print(f"[{index}/{len(mseed_paths)}] elapsed={total:7.1f}s last={fpath}")

    print(f"Collection count = {db.wf_miniseed.count_documents({}):,}")
    db.wf_miniseed.create_index("sta")
    db.wf_miniseed.create_index([("starttime", 1), ("endtime", 1)])
    db.wf_miniseed.create_index("chan")


if __name__ == "__main__":
    main()
