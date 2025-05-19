#!/usr/bin/env python
# coding: utf-8

# # Extended USArray data preprocessing stage 2
# This notebook does the first stage of actual preprocessing of the extended USArray data set.  It should be run after the notebook curently called index_mseed_*.ipynb where the * is normally a year.  It must be run before a related notebook called Preproces2seis_*.ipynb.  After experimenting with several variants this approach seems to be the best solution for performance with a workable memory footprint.   It does a fairly long string of operations all contained in the function atomic_ts_processor.  
# 
# Notice the approach here is processing the data as ensembles grouped by source_id.   The parallel section submits data by ensemble the cluster rather than doing the entire data et in one massive map operator.  That was done because currently dask will not handle bags of that size and we alway seem to abort on a memory fault.   Another reason the ensemble approach is a good one is it allows this job to be rerun if there are problems.  Note how the query that looks for source_id values in the output wf_TimeSeries collection and runs only ids that are not already present.  In the rare case that a job would abort with only part of an ensemble saved, that could drop data but the only way that should happen is if the write hit a file limit.  In that case, we would know as we couldn't continue until the file limit (e.g. quota) was resolved.
# 



# change for different calendar year
year = 2012
dbname = "usarray{}".format(year)
tsdir="./wf_TimeSeries/{}".format(year)
dbmaster="usarray48"




import mspasspy.client as msc
mspass_client = msc.Client(database_name=dbname)
# waveform data indexed to here
db = mspass_client.get_database()
# master database with source and receiver metadata
dbmd = mspass_client.get_database(dbmaster)


# First do imports and define special functions used for this workflow.




from mspasspy.algorithms.window import WindowData
from mspasspy.algorithms.signals import detrend
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.utility import ErrorSeverity
from mspasspy.algorithms.resample import (ScipyResampler,
                                          ScipyDecimator,
                                          resample,
                                         )
from mspasspy.db.normalize import ObjectIdMatcher
from mspasspy.algorithms.calib import ApplyCalibEngine
from mspasspy.util.seismic import number_live
from obspy import UTCDateTime
from obspy.geodetics import gps2dist_azimuth,kilometers2degrees
from obspy.taup import TauPyModel
from dask.distributed import as_completed
import time
import math



def set_PStime(d,Ptimekey="Ptime",Stimekey="Stime",model=None):
    """
    Function to calculate P and S wave arrival time and set times 
    as the header (Metadata) fields defined by Ptimekey and Stimekey.
    Tries to handle some complexities of the travel time calculator 
    returns when one or both P and S aren't calculatable.  That is 
    the norm in or at the edge of the core shadow.  
    
    :param d:  input TimeSeries datum.  Assumes datum's Metadata 
      contains stock source and channel attributes.  
    :param Ptimekey:  key used to define the header attribute that 
      will contain the computed P time.  Default "Ptime".
    :param model:  instance of obspy TauPyModel travel time engine. 
      Default is None.   That mode is slow as an new engine will be
      constructed on each call to the function.  Normal use should 
      pass an instance for greater efficiency.  
    """
    if d.live:
        if model is None:
            model = TauPyModel(model="iasp91") 
        # extract required source attributes
        srclat=d["source_lat"]
        srclon=d["source_lon"]
        srcz=d["source_depth"]
        srct=d["source_time"] 
        # extract required channel attributes
        stalat=d["channel_lat"]
        stalon=d["channel_lon"]
        staelev=d["channel_elev"]
        # set up and run travel time calculator
        georesult=gps2dist_azimuth(srclat,srclon,stalat,stalon)
        # obspy's function we just called returns distance in m in element 0 of a tuple
        # their travel time calculator it is degrees so we need this conversion
        dist=kilometers2degrees(georesult[0]/1000.0)
        arrivals=model.get_travel_times(source_depth_in_km=srcz,
                                            distance_in_degree=dist,
                                            phase_list=['P','S'])
        # always post this for as it is not cheap to compute
        # WARNING:  don't use common abbrevation delta - collides with data dt
        d['epicentral_distance']=dist
        # these are CSS3.0 shorthands s - station e - event
        esaz = georesult[1]
        seaz = georesult[2]
        # css3.0 names esax = event to source azimuth; seaz = source to event azimuth
        d['esaz']=esaz
        d['seaz']=seaz
        # get_travel_times returns an empty list if a P time cannot be 
        # calculated.  We trap that condition and kill the output 
        # with an error message
        if len(arrivals)==2:
            Ptime=srct+arrivals[0].time
            rayp = arrivals[0].ray_param
            Stime=srct+arrivals[1].time
            rayp_S = arrivals[1].ray_param
            d.put(Ptimekey,Ptime)
            d.put(Stimekey,Stime)
            # These keys are not passed as arguments but could be - a choice
            # Ray parameter is needed for free surface transformation operator
            # note tau p calculator in obspy returns p=R sin(theta)/V_0
            d.put("rayp_P",rayp)
            d.put("rayp_S",rayp_S)
        elif len(arrivals)==1:
            if arrivals[0].name == 'P':
                Ptime=srct+arrivals[0].time
                rayp = arrivals[0].ray_param
                d.put(Ptimekey,Ptime)
                d.put("rayp_P",rayp)
            else:
                # Not sure we can assume name is S
                if arrivals[0].name == 'S':
                    Stime=srct+arrivals[0].time
                    rayp_S = arrivals[0].ray_param
                    d.put(Stimekey,Stime)
                    d.put("rayp_S",rayp_S)
                else:
                    message = "Unexpected single phase name returned by taup calculator\n"
                    message += "Expected phase name S but got " + arrivals[0].name
                    d.elog.log_error("set_PStime",
                                     message,
                                     ErrorSeverity.Invalid)
                    d.kill()
        else:
            # in this context this only happens if no P or S could be calculated
            # That shouldn't ever happen but we need this safety in he event it does
            message = "Travel time calculator failed completely\n"
            message += "Could not calculator P or S phase time"
            d.elog.log_error("set_PStime",
                             message,
                             ErrorSeverity.Invalid)
            d.kill()
    return d





def set_file_path(e,dir=tsdir):
    """
    This function is used to set dir and dfile for this workflow for each 
    ensemble.  Note these are set in the ensemble's metadata container 
    not the members.   We don't set them for members as Database.save_data 
    only uses kwarg dir and dfile with the metadata as a fallback.
    """
    e['dir']=dir
    dfile = "dfile_undefined"
    for d in e.member:
        if d.live and 'dfile' in d:
            dfile=d['dfile']
            base,ext = dfile.split(".")
            dfile=base
            break
    e["dfile"] = dfile
    return e

def dbmdquery(year,padsize=86400.0):
    """
    Constructs a MongoDB query dictionary to use as a query argument for normalization matcher classes.
    (All standard BasicMatcher children have a query argument in the constructor for this purpose.)
    The query is a range spanning specified calendar year.   The interval is extended by padsize 
    seconds.   (default is one day = 86400.0)

    Note this query is appropriate for channel not site.
    """
    # uses obspy's UTCDateTime class to create time range in epoch time using the 
    # calendar strings for convenience
    tstr = "{}-01-01T00:00:00.0".format(year)
    st = UTCDateTime(tstr)
    starttime = st.timestamp - padsize
    tstr = "{}-01-01T00:00:00.0".format(year+1)
    et = UTCDateTime(tstr)
    endtime = et.timestamp + padsize
    # not sure the and is required but better safe than sorry
    query = { "$and" :
             [
                 {"starttime" : {"$lte" : endtime}},
                 {"endtime" : {"$gte" : starttime}}
             ]
    }
    return query

    


# This function is the one used in map operator run on each enemble. 




def atomic_ts_processor(d,decimator,resampler,ttmodel,win,calib_engine):
    """
    This function puts all the processing functions for input TimeSeres (d).  It is called in this 
    workflow in a map operator applied to ensemble members.  
    """
    d = detrend(d,type="constant")
    d = resample(d,decimator,resampler)
    d = set_PStime(d,model=ttmodel)
    if d.live and "Ptime" in d:
        ptime = d["Ptime"]
        d.ator(ptime)
        d = WindowData(d,win.start,win.end)
        d.rtoa()
    d = calib_engine.apply_calib(d)
    return d

    


# This is the driver script comparable to a fortran main.  The algorithm here makes sense only for large ensemles like the extended usarray data set.   We process the data in blocks that are the common source gathers.   That was found to reduce the memory footprint of the processing with a modest cost in overhead to submit each ensemble as a different job to the cluster.  




from mspasspy.ccore.seismic import TimeSeriesEnsemble
from mspasspy.io.distributed import read_distributed_data
ttmodel = TauPyModel(model="iasp91")
t0 = time.time()
# important to note these matchers are loaded from dbmd
# bulk_normalize above will set the matching ids
timequery = dbmdquery(year)
chan_matcher = ObjectIdMatcher(dbmd,
                               query=timequery,
                               collection="channel",
                               attributes_to_load=["_id","lat","lon","elev","hang","vang"],
                              )
tstr1 = "{}-01-01T00:00:00.0".format(year)
tstr2 = "{}-01-01T00:00:00.0".format(year+1)
st1=UTCDateTime(tstr1)
st2=UTCDateTime(tstr2)
srcquery = {"time" : {"$gte" : st1.timestamp, "$lt" : st2.timestamp}}
source_matcher = ObjectIdMatcher(dbmd,
                                query=srcquery,
                                collection="source",
                                attributes_to_load=["_id","lat","lon","depth","time"],
                                load_if_defined=["magnitude"],
                                )
target_sample_rate=20.0
resampler=ScipyResampler(target_sample_rate)
decimator=ScipyDecimator(target_sample_rate)
calib_engine = ApplyCalibEngine(dbmd)
tswin = TimeWindow(-200.0,500.0)
srcidlist_ms=db.wf_miniseed.distinct('source_id')

srcidlist_finished = db.wf_TimeSeries.distinct('source_id')
if len(srcidlist_finished)>0:
    srcidlist=[]
    for sid in srcidlist_ms:
        if sid not in srcidlist_finished:
            srcidlist.append(sid)
else:
    srcidlist = srcidlist_ms


print("Number of distinct sources in wf_miniseed= ",len(srcidlist_ms))
print("Number to process this run=",len(srcidlist))
# reduce size for testing - fails submitting all at once
#sidtmp=[]
#for i in range(10):
#    sidtmp.append(srcidlist[i])
#srcidlist=sidtmp

for sid in srcidlist:
    print("working on  ",sid)
    query = {"source_id" : sid}
    mydata = read_distributed_data(db,
                                   query=query,
                                   collection="wf_miniseed",
                                   normalize=[chan_matcher,source_matcher],
                                   npartitions=280 * 3,
                                  )
    mydata=mydata.map(atomic_ts_processor,
                            decimator,
                            resampler,
                            ttmodel,
                            tswin,
                            calib_engine)
    dlist = mydata.compute()
    print("Number of TimeSeries in this ensemble=",len(dlist))
    # package into ensemble for a faster write 
    ens=TimeSeriesEnsemble(len(dlist))
    for d in dlist:
        ens.member.append(d)
    ens.set_live()
    ens = set_file_path(ens)
    ens = db.save_data(ens,
                       return_data=True,
                       collection="wf_TimeSeries",
                       storage_mode="file",
                       dir=tsdir,
                       data_tag="preprocessed_map",
                       )
    del ens
    
    
t = time.time()
print("Run time = ",t-t0," seconds")













