#!/usr/bin/env python
# coding: utf-8

# # Download Master Metadata
# This notebook creates a master database with all source and receiver metadata loaded in the standard MsPASS collection called "source", "channel", and "site".   The collections are large as they span the entire time range of operation of the TA in the lower 48 states.   To reduce the memory footprint, yearly processing scripts should subset these collections to only those spanning the data time range using the query argument of the normalization matcher classes.  
# 
# Earlier version of the usarray workflow downloaded year blocks of source and receiver metadata.  I realized, however, that that created a mess at the end of preprocessing when we needed to merge all yearly databases to a single master for input to pwmig or related processing of the full data set.  With this model the final merge uses year databases as input with all outputs to the master that is the database created by this notebook.  

import os, glob, sys                         # NEW: imports for year handling and file system operations

# NEW: define global output directory for JSON exports
OUTPUT_DIR = "./output"

def main():

    # NEW: ensure the output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    dbname = "usarray48"
    # FULL-RANGE dates for 2005 through 2016
    mdts = "2005-01-01T00:00:00.0"
    mdte = "2016-01-01T00:00:00.0"
    print(f"Processing metadata for full range ({mdts} to {mdte})")

    # obspy requires data converted to their UTCDateTime object 
    from obspy import UTCDateTime
    starttime = UTCDateTime(mdts)
    endtime   = UTCDateTime(mdte)

    import mspasspy.client as msc
    mspass_client = msc.Client(database_name=dbname)
    db = mspass_client.get_database()

    # ## Source metadata
    # This procedure will download way more sources than what was used in the original download but that is preferable to dropping a lot of data.  There are two complications:
    # 1.  The large size of usarray requires a large range for the core shadow edge.   The alternative would be the more complicated procedure used in the original download where we used a center point.   That is worse, however, as it drops stations that ran continuously during the entire array operation.
    # 2.  I drop the magnitude threshold here from the original download size which drastically increases the source collection size.   Hopefully there will be few if any overlaps and those that are can be resolved.

    from obspy.clients.fdsn import Client
    client = Client("IRIS")

    # lower 48 extreme range size is about 48 degrees - round up to 50
    # point below is geographic center of the lower 48.   Use range of 105 maximum 
    # from that point with a padding factor or 25 degrees.  i.e. distance range 
    # up to 105+25=130 degrees.  Minimum is set to 15 degrees but workflow will need to 
    # do  more to limit near distances to workable range for P receiver functions.
    # 15 should work to allow use of eastern us stations with california earthquake 
    # although that is likely a small number
    lat0 = 39.8
    lon0 = -98.6
    minmag     = 4.5
    minradius  = 15.0
    maxradius  = 130.0

    cat = client.get_events(
        starttime=starttime,
        endtime=endtime,
        latitude=lat0,
        longitude=lon0,
        minradius=minradius,
        maxradius=maxradius,
        minmagnitude=minmag
    )
    # this is a weird incantation suggested by obspy to print a summeary of all the events
    #print(cat.__str__(print_all=True))

    n = db.save_catalog(cat)
    print('number of event entries saved in source collection=', n)

    # ## Receiver Metadata
    # This is pretty much the same as the way we have done this process in tutorials and in the prototype I developed earlier downloading these data by year.   The only difference here is the time range is longer.

    chan = "B*"
    # these define a generous box around the lower 48 states.   It may not 
    # exactly match original download script as it is likely somewhat larger than 
    # the download 
    minlongitude = -140.0
    maxlongitude = -60
    minlatitude  = 22.0
    maxlatitude  = 52.0
    inv = client.get_stations(
        starttime=starttime,
        endtime=endtime,
        minlongitude=minlongitude,
        maxlongitude=maxlongitude,
        minlatitude=minlatitude,
        maxlatitude=maxlatitude,
        format='xml',
        channel='BH?',
        level='response'
    )

    db.save_inventory(inv)

    # These indices should speed some forms of processing.

    import pymongo
    db.site.create_index({'location': '2dsphere'})
    db.channel.create_index({'location': '2dsphere'})
    db.channel.create_index(
        [("net", pymongo.ASCENDING),
         ("sta", pymongo.ASCENDING),
         ("chan", pymongo.ASCENDING)]
    )
    db.channel.create_index("starttime")

    # NEW: export the complete 2005–2016 collections to JSON files in OUTPUT_DIR
    from bson.json_util import dumps
    for coll in ['source', 'site', 'channel']:
        filename = os.path.join(OUTPUT_DIR, f"{coll}.json")
        print(f"Exporting collection '{coll}' to '{filename}'...")
        with open(filename, 'w', encoding='utf-8') as f:
            for doc in db[coll].find():
                f.write(dumps(doc) + '\n')
    print("Full-range export complete.")

if __name__ == "__main__":
    main()
