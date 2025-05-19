#!/usr/bin/env python
# coding: utf-8

"""
Restore the three full-range metadata collections (source, site, channel)
from JSON files back into the MsPASS-wrapped MongoDB database exactly
as they were before export.
"""

import os
from bson import json_util
from mspasspy.client import Client
from mspasspy.db.database import Database

# ——————————————————————————————————————————————————————————————
# Configuration
JSON_DIR = "./output"    # directory holding source.json, site.json, channel.json
DB_NAME  = "usarray48"   # must match the original database name
# ——————————————————————————————————————————————————————————————

# 1) Connect via MsPASS client and drop existing database
client   = Client()
dbclient = client.get_database_client()
dbclient.drop_database(DB_NAME)
print(f"Dropped database {DB_NAME!r}.")

# 2) Create fresh Database handle
db = Database(dbclient, DB_NAME)

# 3) Load one collection from its JSON file
def load_collection(coll_name):
    """
    Read JSON docs from <coll_name>.json in JSON_DIR
    and insert them into the matching MsPASS collection.
    """
    path = os.path.join(JSON_DIR, f"{coll_name}.json")
    if not os.path.isfile(path):
        print(f"  WARNING: {path!r} not found—skipping {coll_name}.")
        return

    msp_coll = getattr(db, coll_name)
    count = 0

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            doc = json_util.loads(line)
            msp_coll.insert_one(doc)
            count += 1

    print(f"  Restored {count} docs into '{coll_name}'.")

# 4) Perform restore for each metadata collection
for name in ["source", "site", "channel"]:
    print(f"Restoring collection '{name}'...")
    load_collection(name)

print("All collections have been restored.")
