#!/usr/bin/env python
# coding: utf-8

# # Extended USArray data preprocessing stage 2
# This notebook does the second stage of preprocessing of the extended USArray data set. It reads common source gathers input previously assumed created in wf_TimeSeries and does the following processing steps:
# 1.  Runs bundle_seed_data to transform the data to a SeismogramEnsemble 
# 2.  Applies the free surface transformation to all ensemble members
# 3.  Runs broadband_snr_QC on all members.  
# 
# Step 3 tends to kill a lot of data but that is a key point.   The cemetery contains the bodies.

# In[1]:


# set to year of data being processed
year = 2012
dbname = "usarray{}".format(year)
wfdir = "./wf_Seismogram/{}".format(year)
dbmaster="usarray48"


# In[2]:


import mspasspy.client as msc
mspass_client = msc.Client(database_name=dbname)
db = mspass_client.get_database()
dbmd = mspass_client.get_database(dbmaster)


# First do imports and define special functions used for this workflow.

# In[3]:


from mspasspy.algorithms.window import WindowData
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.utility import ErrorSeverity
from mspasspy.db.normalize import normalize,ObjectIdMatcher
from mspasspy.algorithms.MTPowerSpectrumEngine import MTPowerSpectrumEngine
from mspasspy.algorithms.bundle import bundle_seed_data
from mspasspy.ccore.seismic import SlownessVector
from mspasspy.algorithms.basic import free_surface_transformation,rotate_to_standard
from mspasspy.algorithms.snr import broadband_snr_QC
from mspasspy.util.seismic import number_live
import dask.bag as dbg
import time
import math



def apply_FST(d,rayp_key="rayp_P",seaz_key='seaz',vp0=6.0,vs0=3.5):
    """
    Apply free surface transformation operator of Kennett (1983) to an input `Seismogram` 
    object.   Assumes ray parameter and azimuth data are stored as Metadata in the 
    input datum.  If the ray parameter or azimuth key are not defined an error 
    message will be posted and the datum will be killed before returning.  
    :param d:  datum to process
    :type d:  Seismogram
    :param rayp_key:   key to use to extract ray parameter to use to compute the 
    free surface transformation operator.  Note function assumes the ray parameter is
    spherical coordinate form:  R sin(theta)/V.   Default is "rayp_P".
    :param seaz_key:   key to use to extract station to event azimuth. Default is "seaz".
    :param vp0:  surface P wave velocity (km/s) to use for free surface transformation 
    :param vs0:  surface S wave velocity (km/s) to use for free surface transformation.
    """
    if d.is_defined(rayp_key) and d.is_defined(seaz_key):
        rayp = d[rayp_key]
        seaz = d[seaz_key]
        # Some basic seismology here.  rayp is the spherical earth ray parameter
        # R sin(theta)/v.  Free surface transformation needs apparent velocity 
        # at Earth's surface which is sin(theta)/v when R=Re.   Hence the following
        # simple convertion to get apparent slowness at surface  sin(theta)/v
        Re=6378.1
        umag = rayp/Re   # magnitude of slowness vector
        # seaz is back azimuth - slowness vector points in direction of propagation
        # with is 180 degrees away from back azimuth
        az = seaz + 180.0
        # component slowness vector components in local coordinates
        ux = umag * math.sin(az)
        uy = umag * math.cos(az)
        # FST implementation requires this special class called a Slowness Vector
        u = SlownessVector(ux,uy)
        d = free_surface_transformation(d,uvec=u,vp0=vp0,vs0=vs0)
    else:
        d.kill()
        message = "one of required attributes rayp_P or seaz were not defined for this datum"
        d.elog.log_error("apply_FST",message,ErrorSeverity.Invalid)
        
    return d



def set_file_path(e,dir=wfdir):
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
            break
    e["dfile"] = dfile
    return e


# In[4]:


# this function was copied from Preprocess2ts - it maybe should be a local .py file
def dbmdquery(year,padsize=86400.0):
    """
    Constructs a MongoDB query dictionary to use as a query argument for normalization matcher classes.
    (All standard BasicMatcher children have a query argument in the constructor for this purpose.)
    The query is a range spanning specified calendar year.   The interval is extended by padsize 
    seconds.   (default is one day = 86400.0)
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


# These are higher level functions map operators.

# In[5]:


def read_tsensembles(source_id,db,data_tag=None):
    """
    Reader for this script. Could be done with read_distributed_data and a query generator 
    to create a list of python dictionaries as input to read_distributed_data.   This does the 
    same thing more or less and is a bit clearer.
    
    """
    print("Debug:  entered read_tsensembles")
    query = {"source_id" : source_id}
    if data_tag:
        query["data_tag"] = data_tag
    n = db.wf_TimeSeries.count_documents(query)
    print("Found ",n," documents for nsemble with source_id=",source_id)
    cursor = db.wf_TimeSeries.find(query)
    ens = db.read_data(cursor,collection="wf_TimeSeries")
    print("Number of members in this ensemble=",len(ens.member))
    return ens

def normalize_ensembles(ens,matcher):
    """
    Workaround for bug in normalize function where decorators won't work.
    This function should be removed when that bug is resolved.
    """
    if ens.live:
        for i in range(len(ens.member)):
            ens.member[i] = normalize(ens.member[i],matcher)
    return ens
    
def process2seis(ens,
            swin,
            nwin,
            signal_engine,
            noise_engine,
                ):
    """
    This is the master processing function for this notebook.   It assumes an input of ens 
    that is a common source gather with required Metadata set in the earlier run with the stage 2 
    (Preprocess2ts) notebook.  In this workflow ens is read in parallel with this functiond oing 
    the processing.  The output is a SeismogramEnsemble run through broadband_snr_QC to set 
    snr metrics.  It also normally kills a large fraction of the data.  

    The approach using a loop over the ensemble was designed to reduce the memory footprint as 
    each member is processed and replaces its parent in the ensemble that is returned.  
    """
    print("Processing TimeSeriesEnsemble with ",len(ens.member)," members")
    ens3c = bundle_seed_data(ens)
    del ens
    N = len(ens3c.member)
    for i in range(N):
        ens3c.member[i] = apply_FST(ens3c.member[i])
        ens3c.member[i] = broadband_snr_QC(ens3c.member[i],
                            component=2,
                            signal_window=swin,
                            noise_window=nwin,
                            use_measured_arrival_time=True,
                            measured_arrival_time_key="Ptime",
                            noise_spectrum_engine=noise_engine,
                            signal_spectrum_engine=signal_engine,
                                )
    return ens3c
    


# This is the driver script comparable to a fortran main.

# In[ ]:


t0 = time.time()
#chanquery=dbmdquery(year)
chan_matcher = ObjectIdMatcher(dbmd,
                               collection="channel",
                               attributes_to_load=["_id","lat","lon","elev","hang","vang"],
                              )

# for usage on IU system each node has 2*64=128 cores so using 128 workers for the run
# makes this twice that
npartitions=20
swin = TimeWindow(-5.0,100.0)
nwin = TimeWindow(-195.0,-5.0)
# spectrum estimation engines for broadband_snr_QC
dt = 0.05
nsamp_noise = int((nwin.end-nwin.start)/dt) + 1
nsamp_sig = int((swin.end-swin.start)/dt) + 1
signal_engine=MTPowerSpectrumEngine(nsamp_sig,5.0,8,2*nsamp_sig,dt)
noise_engine=MTPowerSpectrumEngine(nsamp_noise,5.0,10,2*nsamp_noise,dt)
srcidlist_ms=db.wf_TimeSeries.distinct('source_id')
print(len(srcidlist_ms)," distinct source_id values in wf_TimeSeries")
srcidlist_finished = db.wf_Seismogram.distinct('source_id')
if len(srcidlist_finished)>0:
    srcidlist=[]
    for sid in srcidlist_ms:
        if sid not in srcidlist_finished:
            srcidlist.append(sid)
else:
    srcidlist = srcidlist_ms


print("Number of distinct sources in wf_TimeSeries= ",len(srcidlist_ms))
print("Number to process this run=",len(srcidlist))
# reduce size for testing 
#sidtmp=[]
#for i in range(4):
#    sidtmp.append(srcidlist[i])
#srcidlist=sidtmp
#print("Debug process list:  ",srcidlist)
mydata = dbg.from_sequence(srcidlist)
mydata = mydata.map(read_tsensembles,db)
# note default behavior for normalize is to normalize all members
#mydata = mydata.map(normalize,chan_matcher,handles_ensembles=False)
#workaround for above until bug is fixed in normalize
mydata = mydata.map(normalize_ensembles,chan_matcher)
mydata = mydata.map(process2seis,
                      swin,
                      nwin,
                      signal_engine,
                      noise_engine,
                               )
mydata = mydata.map(set_file_path)
mydata = mydata.map(db.save_data,
                    collection="wf_Seismogram",
                    storage_mode="file",
                    dir=wfdir,
                    data_tag="FST_and_QCed",
                   )
out=mydata.compute()
print(out)
t = time.time()
print("Run time = ",t-t0," seconds")






