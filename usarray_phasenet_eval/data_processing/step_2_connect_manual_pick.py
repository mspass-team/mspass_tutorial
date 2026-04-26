#!/usr/bin/env python3
"""Attach Datascope manual P picks to indexed USArray miniSEED documents."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from time import time

from mspasspy.db.client import DBClient
from mspasspy.preprocessing.css30.datascope import DatascopeDatabase
from pymongo import UpdateOne


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
        description="Join Datascope manual picks with indexed USArray miniSEED metadata."
    )
    parser.add_argument("year", type=int, help="Four-digit year, for example 2015")
    parser.add_argument(
        "--manual-pick-root",
        type=Path,
        default=os.environ.get("USARRAY_MANUAL_PICK_ROOT"),
        required="USARRAY_MANUAL_PICK_ROOT" not in os.environ,
        help="Directory containing monthly Datascope pick databases. "
        "Can also be set with USARRAY_MANUAL_PICK_ROOT.",
    )
    parser.add_argument(
        "--datascope-pf",
        default="data/pf/DatascopeDatabase.pf",
        help="Datascope parameter file passed to DatascopeDatabase.",
    )
    parser.add_argument(
        "--mongo-host",
        default=os.environ.get("USARRAY_MONGO_HOST", "localhost"),
        help="MongoDB host. Can also be set with USARRAY_MONGO_HOST.",
    )
    parser.add_argument("--mongo-port", type=int, default=27020)
    parser.add_argument("--mongo-db-prefix", default="usarray")
    parser.add_argument("--batch-size", type=int, default=1000)
    return parser.parse_args()


def monthly_pick_path(root: Path, year: int, month: int) -> Path:
    month_str = f"{month:02d}"
    return root / f"{year}_{month_str}" / f"usarray_{year}_{month_str}"


def load_monthly_manual_picks(args: argparse.Namespace) -> list:
    manual_pick_tables = []

    for month in range(1, 13):
        pick_path = monthly_pick_path(args.manual_pick_root, args.year, month)
        database = DatascopeDatabase(str(pick_path), pffile=args.datascope_pf)
        manual_pick_tables.append(database.get_table("arrival"))
        print(f"Loaded manual picks: {pick_path}")

    return manual_pick_tables


def print_collection_indexes(db) -> None:
    for index in db.wf_miniseed.list_indexes():
        print(f"Index Name: {index['name']}")
        print(f" - Keys: {index['key']}")
        print(f" - Unique: {index.get('unique', False)}")
        print(f" - Sparse: {index.get('sparse', False)}")
        print(f" - TTL: {index.get('expireAfterSeconds')}")


def clear_existing_manual_picks(db) -> None:
    query = {"chan": {"$regex": "Z$"}}
    vertical_count = db.wf_miniseed.count_documents(query)
    print(f"Documents with vertical channels: {vertical_count:,}")

    result = db.wf_miniseed.update_many(
        {"manual_picks": {"$exists": True}},
        {"$unset": {"manual_picks": ""}},
    )
    print(f"Removed manual_picks from {result.modified_count:,} documents")
    remaining = db.wf_miniseed.count_documents({"manual_picks": {"$exists": True}})
    print(f"Documents still containing manual_picks: {remaining:,}")


def match_mseed_with_manual_picks(db, manual_picks_df, month: int, batch_size: int) -> None:
    print(f"Processing month {month:02d}")
    print(f"Manual pick count: {len(manual_picks_df):,}")

    vertical_picks = manual_picks_df[manual_picks_df["chan"].str.endswith("Z")]
    print(f"Vertical-channel manual pick count: {vertical_picks.shape[0]:,}")

    start = time()
    matched_docs = 0

    for pick_index, (_, row) in enumerate(vertical_picks.iterrows(), 1):
        query = {
            "sta": {"$eq": row["sta"]},
            "starttime": {"$lt": row["time"]},
            "endtime": {"$gt": row["time"]},
        }

        bulk_ops = []
        cursor = db.wf_miniseed.find(query, batch_size=batch_size)
        for doc in cursor:
            bulk_ops.append(
                UpdateOne({"_id": doc["_id"]}, {"$push": {"manual_picks": row["time"]}})
            )
            matched_docs += 1

            if len(bulk_ops) >= batch_size:
                db.wf_miniseed.bulk_write(bulk_ops)
                bulk_ops = []

        if bulk_ops:
            db.wf_miniseed.bulk_write(bulk_ops)

        if pick_index % 10_000 == 0:
            elapsed = time() - start
            print(
                f"Processed {pick_index:,} picks, "
                f"matched {matched_docs:,} docs in {elapsed:.1f}s"
            )


def print_manual_pick_distribution(db) -> None:
    pipeline = [
        {
            "$project": {
                "manual_pick_count": {
                    "$cond": [{"$isArray": "$manual_picks"}, {"$size": "$manual_picks"}, 0]
                }
            }
        },
        {"$group": {"_id": "$manual_pick_count", "count": {"$sum": 1}}},
        {"$sort": {"_id": -1}},
    ]

    for row in db.wf_miniseed.aggregate(pipeline):
        print(f"manual_picks length = {row['_id']}: {row['count']} docs")


def main() -> None:
    args = parse_args()
    mongo_db_name = f"{args.mongo_db_prefix}{args.year}"

    section("1. Load manual picks")
    manual_pick_tables = load_monthly_manual_picks(args)

    section("2. Connect to MongoDB")
    db = DBClient(f"{args.mongo_host}:{args.mongo_port}").get_database(mongo_db_name)
    print_collection_indexes(db)

    section("3. Clear existing manual picks")
    clear_existing_manual_picks(db)

    section("4. Join manual picks")
    for month, manual_picks_df in enumerate(manual_pick_tables, 1):
        match_mseed_with_manual_picks(db, manual_picks_df, month, args.batch_size)

    section("5. Manual pick distribution")
    print_manual_pick_distribution(db)


if __name__ == "__main__":
    main()
