#!/usr/bin/env python
# coding: utf-8

# # Stage 1:  Build wf_miniseed 
# This notebook is the first step for processing the extended usarray data set in year segments. It runs the indexing program to build a set of index documents in the wf_miniseed collection.  It then runs bulk_normalize to create the channel_id and site_id cross references in the wf_miniseed documents.   That is essential for the second stage of the processing.
# 
# This is a separate notebook because prototypes demonstrated:
# 1.  There are too many potential issues with miniseed data that can cause problems that is is useful to checkpoint the job at the end of the notebook to verify things are ok. That is particularly  true of normalizations and potential miniseed data problems.
# 2.  This notebook is note efficient to runw with a larger number of workers like the subsequent notebooks.   Running in with only 8 workers or so with a small memory requirement can be it through faster than waiting for a larger job requiring more resources, which is the case for the notebooks run after this one. 


# change for different calendar year
year = 2012
dbname = "usarray{}".format(year)
dbmaster="usarray48"



import mspasspy.client as msc
mspass_client = msc.Client(database_name=dbname)
# waveform data indexed to here
db = mspass_client.get_database()
# master database with source and receiver metadata
dbmd = mspass_client.get_database(dbmaster)




# This builds a file list to drive index processing
import os
import fnmatch
import dask.bag as dbg
import time
topdirectory="./wf"
# assume year was defined at the top and data have the structure of wf/year/*.mseed
dir="{}/{}".format(topdirectory,year)
filelist=fnmatch.filter(os.listdir(dir),'*.mseed')
print("Number of mseed files to process=",len(filelist))
tstart=time.time()
data = dbg.from_sequence(filelist)
data = data.map(db.index_mseed_file,dir)
data=data.compute()
tend=time.time()
print("Elapsed time to run index_mseed_file=",tend-tstart)
# TODO: wf_miniseed collection number of documents


# Normalization of the mseed records is complicated by the fact we are using two databases here.  The current normalize_mseed will not work because it assumes one db holds wf_miniseed and the channel-site collections.  For that reason I use bulk_normalize which is more generic.  That, of course, is why the first part of this block creates the MiniseedMatcher and OriginTimeMatcher instance. 
# 
# Note the memory footprint of this job could be reduced by using the query parameter for the constructors for MiniseedMatcher and OriginTimeMatcher, but since this section is a serial job and the objects aren't that huge anyway I don't bother.   



from mspasspy.db.normalize import MiniseedMatcher,OriginTimeMatcher,bulk_normalize
# prepend_collection_name defaults to True but best to be clear that is essential here
chan_matcher = MiniseedMatcher(dbmd,
                               collection="channel",
                               prepend_collection_name=True,
                              )
site_matcher = MiniseedMatcher(dbmd,
                               collection="site",
                               attributes_to_load=["_id","lat","lon","elev","starttime","endtime"],
                               prepend_collection_name=True,
                              )
source_matcher = OriginTimeMatcher(dbmd,
                                   t0offset=0.0,
                                   tolerance=100.0,
                                   attributes_to_load=['_id','time'])
bno=bulk_normalize(db,wf_col="wf_miniseed",matcher_list=[chan_matcher,site_matcher,source_matcher])



print("Number of channel_id values set=",bno[1])
print("Number of site_id values set=",bno[2])
print("Number of source_id values set=",bno[3])
print("Number of documents processed=",bno[0])

