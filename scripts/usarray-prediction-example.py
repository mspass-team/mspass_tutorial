import time
import numpy as np
import pandas as pd
from mspasspy.preprocessing.css30.datascope import DatascopeDatabase
from time import time

year = 2015
month = 12
usarray_folder = "/scratch1/07114/jiaoma/usarray_2015/2015"
usarray_manual_pick_fname = (
    "/scratch1/07114/jiaoma/mspass/workdir/manual_picks/events_usarray_2015_12/usarray_2015_12"
)
mongo_host = "c205-027"
mongo_port = 27020
mongo_db_name = "usarray{}".format(year)
pffile = "data/pf/DatascopeDatabase.pf"

def section(title: str, width: int = 80, char: str = "#") -> None:
    """Print a centered banner that doubles as an in‑code section header.

    Example
    -------
    section("Step 1. load manual pick")

    # Produces:
    ########################################################################
    #                         Step 1. load manual pick                     #
    ########################################################################
    """
    if width < len(title) + 4:
        width = len(title) + 4  # ensure room for padding / edge chars
    pad_total = width - len(title) - 2  # 2 edge chars
    left_pad = pad_total // 2
    right_pad = pad_total - left_pad

    line = char * width
    middle = f"{char}{' ' * left_pad}{title}{' ' * right_pad}{char}"

    print(line)
    print(middle)
    print(line)


################################################################################
# Step 1. load manual pick
################################################################################
section("Step 1. load manual pick")
dsd = DatascopeDatabase(usarray_manual_pick_fname, pffile=pffile)
manual_picks_df = dsd.get_table("arrival")


################################################################################
# Step 2. index usarray data
################################################################################
section("Step 2. index usarray data")
from mspasspy.db.client import DBClient
import os
import fnmatch

dbclient = DBClient("{}:{}".format(mongo_host, str(mongo_port)))
db = dbclient.get_database(mongo_db_name)

all_selected_files = []
file_list = fnmatch.filter(os.listdir(usarray_folder), "*.mseed")
all_selected_files.extend([os.path.join(usarray_folder, f) for f in file_list])
print("{} mseed files in scope".format(len(all_selected_files)))

total_time = 0
file_counter = 0

# extract the indexing part as a separate script
# for dfile in all_selected_files:
#     t0 = time()
#     db.index_mseed_file(dfile, dir)
#     t1 = time()
#     dt = t1 - t0
#     total_time += dt

#     file_counter += 1
#     if file_counter % 10 == 0:
#         print(
#             "Current count: {}, time elapsed: {:.2f}s, file: {}".format(
#                 file_counter, total_time, dfile
#             )
#         )

#         # to remove after testing
#         if file_counter > 100:
#             break

print("Total number of documents in mongo collection: {}".format(db.wf_miniseed.count_documents({})))

db.wf_miniseed.find_one()

db.wf_miniseed.create_index("sta")
db.wf_miniseed.create_index(["starttime", "endtime"])
db.wf_miniseed.create_index("chan")

# index details of the collection
for index in db.wf_miniseed.list_indexes():
    print(f"Index Name: {index['name']}")
    print(f" - Keys: {index['key']}")
    print(f" - Unique: {index.get('unique', False)}")
    print(f" - Sparse: {index.get('sparse', False)}")
    print(f" - TTL: {index.get('expireAfterSeconds')}")


################################################################################
# Step 3. Clean up
################################################################################
section("Step 3. Clean up")
# manual pick details
print("manual pick total count: {}".format(len(manual_picks_df)))
z_mp_df = manual_picks_df[manual_picks_df["chan"].str.endswith("Z")]
print("manual pick 'chan' ends with Z count: {}".format(z_mp_df.shape[0]))

# find documents that have channel as %Z, Z (vertical) is most important for p-wave pick
query = {"chan": {"$regex": "Z$"}}
n = db.wf_miniseed.count_documents(query)
print("Number of documents that have chan as %Z = ", n)

# clean up the manual_picks field
db.wf_miniseed.update_many(
    {"manual_picks": {"$exists": True}}, {"$unset": {"manual_picks": ""}}
)
# list the number of documents that have manual_picks field
print(
    "Number of documents that have manual_picks field: {}".format(
        db.wf_miniseed.count_documents({"manual_picks": {"$exists": True}})
    )
)


################################################################################
# Step 4. join manual pick with usarray data
################################################################################
section("Step 4. join manual pick with usarray data")
from pymongo import UpdateOne

ct = 0
st = time()
batch_size = 1000

for i, (index, row) in enumerate(z_mp_df.iterrows()):
    sta = row["sta"]
    pick_time = row["time"]
    query = {
        "sta": {"$eq": sta},
        "starttime": {"$lt": pick_time},
        "endtime": {"$gt": pick_time},
    }
    cursor = db.wf_miniseed.find(
        query, batch_size=batch_size
    )  # adjust batch size as needed

    bulk_ops = []
    for doc in cursor:
        bulk_ops.append(
            UpdateOne({"_id": doc["_id"]}, {"$push": {"manual_picks": pick_time}})
        )
        ct += 1

        # Execute batch every `batch_size` items
        if ct % batch_size == 0:
            db.wf_miniseed.bulk_write(bulk_ops)
            bulk_ops = []

    # Write any remaining ops
    if bulk_ops:
        db.wf_miniseed.bulk_write(bulk_ops)

    if i % 1000 == 0:
        print(f"Processed no. {i} manual pick, time elapsed: {time() - st} s")

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

# print the stats of documents that have manual_picks
results = list(db.wf_miniseed.aggregate(pipeline))
for r in results:
    print(f"manual_picks length = {r['_id']}: {r['count']} docs")


################################################################################
# Step 5. Prediction 1 - predict arrival time for timeseries with non-empty manual picks
################################################################################
section("Step 5. Prediction 1 - predict arrival time for timeseries with non-empty manual picks")
from mspasspy.algorithms.ml.arrival import annotate_arrival_time
import seisbench.models as sbm
model = sbm.PhaseNet.from_pretrained("original")

non_empty_picks_filter = {
    "manual_picks.0": {"$exists": True},
    "chan": "BHZ"
}

cursor = db.wf_miniseed.find(non_empty_picks_filter, batch_size=100)

has_predict = 0
total_ts = 0

for doc in cursor:
    total_ts += 1
    try:
        ts = db.read_data(doc)
        annotate_arrival_time(ts, 0.1)
        if "p_wave_picks" in ts and len(ts["p_wave_picks"]) > 0:
            has_predict += 1
    except Exception as e:
        pass

print("{} out of {} timeseries had a prediction".format(has_predict, total_ts))


################################################################################
# Step 6. Prediction 2 - group timeseries to stream
################################################################################
section("Step 6. Prediction 2 - group timeseries to stream")
# ------------------------------------------------------------------
# 1️⃣  Docs with exactly ONE manual_pick + BH% channel
# ------------------------------------------------------------------
filter_stage = {
    "manual_picks.0": {"$exists": True},  # array with at least 1 element
    "manual_picks.1": {"$exists": False}, # but NOT a 2nd element ➜ exactly one pick
    "chan": {"$in": ["BHZ", "BHE", "BHN"]}
}

n_filtered = db.wf_miniseed.count_documents(filter_stage)
print(f"[STEP 1] candidate documents with one manual_pick + BH? channel: {n_filtered:,}")

# ------------------------------------------------------------------
# 2️⃣  Group by sta/start/end/dfile (all candidates)
# ------------------------------------------------------------------
group_stage = [
    { "$match": filter_stage },
    { "$group": {
        "_id": {
            "sta":       "$sta",
            "starttime": "$starttime",
            "endtime":   "$endtime",
            "dfile":     "$dfile"
        },
        "ids":    { "$addToSet": "$_id" },
        "chans":  { "$addToSet": "$chan" }
    }}
]

groups = list(db.wf_miniseed.aggregate(group_stage, allowDiskUse=True))
print(f"[STEP 2] unique sta/start/end/dfile groups formed: {len(groups):,}")

# ------------------------------------------------------------------
# 3️⃣  Keep only COMPLETE BHZ+BHE+BHN triplets
# ------------------------------------------------------------------
complete_groups = [
    g for g in groups
    if set(g["chans"]) == {"BHZ", "BHE", "BHN"}
]

print(f"[STEP 3] groups with ALL three channels: {len(complete_groups):,}")
print(f"         (dropped {len(groups) - len(complete_groups):,} incomplete groups)")

# ------------------------------------------------------------------
# 4️⃣  Build the in-memory lookup dict
# ------------------------------------------------------------------
triplets = {}   # key ➜ list[ObjectId]

for g in complete_groups:
    key = (
        g["_id"]["sta"],
        g["_id"]["starttime"],
        g["_id"]["endtime"],
        g["_id"]["dfile"],
    )
    triplets[key] = g["ids"]

print(f"[STEP 4] triplets dict built with {len(triplets):,} entries ✅")

# ------------------------------------------------------------------
# 5️⃣  Example usage
# ------------------------------------------------------------------
if triplets:
    sample_key = next(iter(triplets))
    sample_ids = triplets[sample_key]
    print("A sample of the triplets formed:")
    print("\nSample key:", sample_key)
    print("Channel IDs:", sample_ids)

    # pull the full docs when needed
    docs = list(db.wf_miniseed.find({"_id": {"$in": sample_ids}}))
    print("Channels in sample doc set:", [d["chan"] for d in docs])

from obspy import Stream
from mspasspy.db.database import Database
from mspasspy.util.converter import TimeSeries2Trace


# form stream based on the triplets
def triplet_to_stream(db: Database, oid_z, oid_e, oid_n) -> Stream:
    """
    Convert three MsPASS TimeSeries docs (Z, E, N) into one ObsPy Stream.

    • Enforces consistent sample counts / start times.
    • Returns Stream with Traces ordered [BHZ, BHE, BHN].
    """

    # Load MsPASS TimeSeries objects
    ts_z = db.read_data(oid_z)
    ts_e = db.read_data(oid_e)
    ts_n = db.read_data(oid_n)

    # Convert each to ObsPy Trace
    tr_z = TimeSeries2Trace(ts_z)
    tr_e = TimeSeries2Trace(ts_e)
    tr_n = TimeSeries2Trace(ts_n)

    # ---- sanity checks (optional but wise) ------------------------
    sr = {tr.stats.sampling_rate for tr in (tr_z, tr_e, tr_n)}
    if len(sr) != 1:
        raise ValueError(f"Sampling rates differ: {sr}")

    npts = {tr.stats.npts for tr in (tr_z, tr_e, tr_n)}
    if len(npts) != 1:
        # Align to common intersection if lengths drift by ≤1 sample
        min_n = min(npts)
        for tr in (tr_z, tr_e, tr_n):
            tr.data = tr.data[:min_n]
            tr.stats.npts = min_n

    # Sort so downstream code can assume [Z, E, N]
    traces = sorted([tr_z, tr_e, tr_n],
                    key=lambda t: t.stats.channel)  # BH[E,N,Z] α-sort puts Z last
    # If you prefer strict Z, E, N order regardless of alpha-sort:
    trace_map = {tr.stats.channel: tr for tr in [tr_z, tr_e, tr_n]}
    traces   = [trace_map[ch] for ch in ("BHZ", "BHE", "BHN")]

    return Stream(traces)

streams = []
for k, v in triplets.items():
    docs = list(db.wf_miniseed.find({"_id": {"$in": v}}))
    ts_z = None
    ts_e = None
    ts_n = None
    for doc in docs:
        if doc["chan"] == 'BHZ':
            ts_z = doc
        elif doc["chan"] == 'BHE':
            ts_e = doc
        elif doc["chan"] == 'BHN':
            ts_n = doc
        else:
            raise ValueError("not expected")
    st = triplet_to_stream(db, ts_z, ts_e, ts_n)
    streams.append(st)
print("stream len: {}".format(len(streams)))

################################################################################
# Step 7. Prediction 2 - predict arrival time for streams
################################################################################
section("Step 7. Prediction 2 - predict arrival time for streams")
def is_over_threshold(data, threshold):
    indices = np.where(data >= threshold)[0]
    return int(indices.size > 0)

over_dot_1 = 0
over_dot_5 = 0
high_prob_stream = None
high_prob_annotated_stream = None

for stream in streams:
    # see if there is an annotation with high confidence in all streams
    pred_st = model.annotate(stream)
    
    for tr in pred_st:
        if tr.stats.channel == "PhaseNet_P":
            trace = tr
            break
    assert trace is not None
    data = trace.data

    # Step 2: Find all the index with probability value greater than the threshold
    over_dot_1 += is_over_threshold(data, 0.1)
    if_dot_5 = is_over_threshold(data, 0.5)
    if if_dot_5:
        high_prob_stream = stream
        high_prob_annotated_stream = pred_st
        over_dot_5 += 1

print("Over {} streams, {} has p wave arrival pick confidence over {}".format(len(streams), over_dot_1, 0.1))
print("Over {} streams, {} has p wave arrival pick confidence over {}".format(len(streams), over_dot_5, 0.5))


################################################################################
# Step 8. Prediction 2 - plot a prediction with high probability
################################################################################
section("Step 8. Prediction 2 - plot a prediction with high probability")
from matplotlib.dates import date2num
from obspy import UTCDateTime
import matplotlib.dates as mdates
from mspasspy.util.converter import Trace2TimeSeries
import matplotlib.pyplot as plt
from pathlib import Path

def plot(stream, annotations, highlight_time, outfile: str, dpi: int = 150):
    fig, axs = plt.subplots(2, 1, sharex=True, figsize=(15, 10),
                            gridspec_kw={'hspace': 0})

    st0_mpl = stream[0].stats.starttime.matplotlib_date  # scalar
    to_mpl  = lambda secs: st0_mpl + secs / 86400.0      # vector-safe

    # Waveforms
    for tr in stream:
        axs[0].plot(to_mpl(tr.times()), tr.data, label=tr.stats.channel)

    # Annotation curves
    offset = annotations[0].stats.starttime - stream[0].stats.starttime
    for tr in annotations:
        if tr.stats.channel[-1] == "N":
            continue
        axs[1].plot(to_mpl(tr.times() + offset), tr.data, label=tr.stats.channel)

    # --------------------  highlight  --------------------
    if highlight_time:
        x = date2num(highlight_time.datetime)
        for ax in axs:
            ax.axvline(x, color='red', ls='--', lw=1.5)
        axs[0].annotate("Pick", xy=(x, 0), xycoords=('data', 'axes fraction'),
                        xytext=(5, 5), textcoords='offset points',
                        color='red', rotation=90, va='bottom')

    # Cosmetics
    axs[0].legend(); axs[1].legend()
    axs[0].set_ylabel('Amplitude'); axs[1].set_ylabel('Picker score')
    axs[1].set_xlabel('UTC time')
    axs[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    fig.autofmt_xdate()
    plt.show()

    outfile = Path(outfile).expanduser().resolve()
    outfile.parent.mkdir(parents=True, exist_ok=True)

    plt.savefig(outfile, dpi=dpi, bbox_inches="tight")
    print(f"Figure saved ➜ {outfile}")

# ---------------- usage -----------------

# use one timeseries of the stream to get the manual pick
ts = Trace2TimeSeries(high_prob_stream[0])
doc = db.wf_miniseed.find_one({
    "sta":       ts['sta'],
    "starttime": ts['starttime'],
    "endtime":   ts['endtime'],
    "dfile":     ts['dfile'],
    "chan":      ts['chan']
})
assert doc is not None
p_pick_time = UTCDateTime(doc["manual_picks"][0])

plot(high_prob_stream, high_prob_annotated_stream,
     highlight_time=p_pick_time, outfile='plots/high_prob_stream_pred.png')