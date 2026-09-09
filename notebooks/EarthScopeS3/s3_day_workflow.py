"""Driver completion for the contributed daily-pickle workflow.

This preserves one ordinary TimeSeriesEnsemble pickle per day, including the
scratch URI's user prefix. It does not make a whole day a bounded-size task.
"""

import pickle

from bson.objectid import ObjectId
from mspasspy.ccore.seismic import TimeSeriesEnsemble
from mspasspy.db.database import elog2doc
from mspasspy.util.db_utils import fetch_dbhandle


def merge_ensembles(enslist, yearday_key="jday_tag"):
    """Merge in order and consume the input list, replacing entries with None.

    Called only for a disposable worker result. A Python parameter deletion
    cannot release the pipeline's reference to that list; clearing its entries
    releases source ensembles incrementally. Members are still copied into the
    output, but the complete input need not coexist with serialization state.
    """
    count = sum(len(ens.member) for ens in enslist if ens.live)
    merged = TimeSeriesEnsemble(count)
    failures = []
    for index in range(len(enslist)):
        ens = enslist[index]
        try:
            if ens.live:
                if yearday_key not in merged and yearday_key in ens:
                    merged[yearday_key] = ens[yearday_key]
                for member in ens.member:
                    merged.member.append(member)
                # A pybind member wrapper can keep its source vector alive.
                if len(ens.member):
                    del member
            else:
                document = dict(ens)
                document["elog_content"] = elog2doc(ens.elog)
                failures.append(document)
        finally:
            enslist[index] = None
            del ens
    if len(merged.member):
        merged.set_live()
    return [merged, failures]


def save_jday_outputs(
    reader_output,
    dbname_or_handle,
    s3fshandle,
    bucket,
    year,
    yearday_key="jday_tag",
    s3_fail_collection="s3_read_failures",
    verbose=False,
):
    """Consume reader_output and save a daily ensemble; return whether saved.

    bucket is the full SCRATCH_BUCKET URI, potentially including a user prefix.
    An empty/dead day returns False after recording read failures. Database and
    upload errors propagate unchanged so a failed write cannot look successful.
    The caller owns the filesystem and database handles.
    """
    if not isinstance(bucket, str) or not bucket.strip():
        raise ValueError("SCRATCH_BUCKET must be a nonempty bucket/prefix URI")
    db = fetch_dbhandle(dbname_or_handle)
    ens, failures = merge_ensembles(reader_output, yearday_key=yearday_key)
    nfailed = len(failures)
    if failures:
        db[s3_fail_collection].insert_many(failures)
    del failures
    if ens.dead():
        return False
    if yearday_key in ens:
        basename = ens[yearday_key]
    else:
        basename = str(ObjectId())
        print(f"Missing {yearday_key}; using unique output name {basename}")
    object_key = f"{bucket.rstrip('/')}/{year}/{basename}.pickle"
    if verbose:
        print(f"Saving {len(ens.member)} members to {object_key}; failures={nfailed}")
    with s3fshandle.open(object_key, "wb") as output:
        pickle.dump(ens, output)
    return True


def count_saved(total, saved):
    """Count successful day files with constant-size pipeline state."""
    return (0 if total is None else total) + int(saved)
