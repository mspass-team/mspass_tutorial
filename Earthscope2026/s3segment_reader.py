#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prototype module to use earthscope sdk to fetch waveform segments 
from AWS s3 storage.  Depends upon the structure of s3 used 
by Earthscope that is different from SCEDC and NCEDC.   

This module has three distinctly different components that might 
be split into three modules in a released version.  The components are:
    1.  An indexer that creates a wf_s3 index collection for desired data
    2.  A reformatter that take a DataFrame of input arrival time 
        data and creates a list of documents that can drive downloading.
    3.  Actual fetch code that handles complexities in actually using 
        the output of 2.   
        
Emphasize this is a prototype code and will likely evolve before release.
Another reason it may evolve is that the earthscope sdk does not appear to 
be completely stable.  

Created on Tue Apr  7 05:27:47 2026

@author: pavlis
"""
import io
import obspy
from obspy import UTCDateTime
import pandas as pd
import calendar
import asyncio
#from mspasspy.util.db_utils import fetch_dbhandle
from mspasspy.util.db_utils import fetch_dbhandle
from mspasspy.ccore.seismic import TimeSeriesEnsemble
from mspasspy.util.converter import Stream2TimeSeriesEnsemble
from mspasspy.util.seismic import number_live
from mspasspy.algorithms.window import WindowData
import dask.distributed as ddist
from mspasspy.ccore.utility import ErrorLogger,ErrorSeverity
from botocore.exceptions import ClientError,BotoCoreError
# this will change when this is in the mspass distribution
from s3_worker_plugin import fetch_s3_client
BUCKET="earthscope-mseed-res-na3mtd4fq5kz7pntcyr1uh46use2a--ol-s3"


async def fetch_net_day_list(s3_client,
                       net,
                       year,
                       day,
                       bucket=BUCKET,
		       strip_version=True,
                       )->list:
    """
    Fetch the raw list of object names from s3 for network net for year and julian day day.  

    Returns a list of strings.  Drops other metadata returning only the string defining 
    s3 object names.
    
    Note s3_client is assumed to be an AsyncEarthScopeClient which is 
    async.   That is why the function signature is async.   As usual that 
    means calling this function requires an await construct.
    """
    base_prefix = "miniseed"
    prefix = f"{base_prefix}/{net}/{year}/{day:03d}/"
    # works for now - BUCKET needs to be an arg
    list_resp = await s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    keys=[]
    for obj in list_resp['Contents']:
        rawkey = obj['Key']
        keys.append(rawkey)
        if strip_version:
            k = rawkey.split("#")
            key2use=k[0]
        else:
            key2use = rawkey
        keys.append(key2use)
    return keys

def objlist2doclist(objlist,auxmd=None):
    """
    Splits s3 object names into dictionary entries to create a document to be saved in MongoDB. 
    Use dictionary like object *auxmd* to load additional, constant metadata to each doc returned.
    """
    doclist=list()
    for objname in objlist:
        doc=dict()
        doc["s3key"] = objname
        s = objname.split("/")
        doc["net"] = s[1]
        yr = int(s[2])
        doc["year"] = yr
        jday = int(s[3])
        doc["jday"] = jday
        starttime = UTCDateTime(year=yr,julday=jday)
        doc["starttime"] = starttime.timestamp
        endtime = starttime.timestamp + 86400.0
        doc["endtime"] = endtime
        s2 = s[4].split(".")
        doc["sta"] = s2[0]
        if auxmd is not None:
            for k in auxmd.keys():
                # this maybe should warn if k overwrites an existing key
                doc[k] = auxmd[k]
        doclist.append(doc)
    return doclist

def save_s3docs(doclist,dbname_or_handle,collection="wf_s3"):
    """
    Simple function to save doclist produced by objlist2doclist to defined by 
    argument dbname_or_handle.  The function calls the MsPASS function 
    fetch_dbhandle which allows this function to be run serial or parallel. 
    The arg should be string defining the database name in parallel mode.
    For serial processing use an instance of a Database object. 
    """
    # guard against this null case - just return an empty list 
    if len(doclist)==0:
        return list()
    db = fetch_dbhandle(dbname_or_handle)
    # this line can throw a range of exceptions.   A final robust implementation 
    # needs a series of error handlers to make this bombproof
    # normal return is a special MongoDB class called a InsertManyResult that is list like
    result = db[collection].insert_many(doclist)
    return result

def days_needed(t0,start,end,pad=100.0)->list:
    """
    Computes a range of days from a time interval t0+start to t0+tend.  
    Optional pad in seconds.  That is, normally the function returns a list of 
    year-day pairs defined as the range t0+start-pad to t0+end+pad.   
    That is necessary because miniseed day files usually have inexact start times 
    due to compresssion.

    The idea of the function is that t0 is either a measured or theoretical 
    phase arrival time and start:end is the time window around that time 
    that we aim to extract.   Downstream code has to take this output 
    and design an efficient read strategy for groups of arrivals   
    
    Return as a list of (year,day) tuples.  
    """
    if end<start:
        message = "days_needed:  start time={} is less than end time={}".format(start,end)
        raise ValueError(message)
    tsutc = UTCDateTime(t0 + start - pad)
    teutc = UTCDateTime(t0 + end + pad)
    result = list()
    if tsutc.year==teutc.year:
        yr = tsutc.year
        for jday in range(tsutc.julday,teutc.julday+1):
            result.append([yr,jday])
    else:
        # complexity required to cross year boundary
        # first a sanity check 
        if (teutc.year-tsutc.year)!=1:
            message="days_needed:  received irrational input.\n"
            message += "Time window of {} -> {} spans multiple years\n".format(tsutc,teutc)
            message += "Irrational input as the volume of data to download is too large"
            raise ValueError(message)
        if calendar.isleap(tsutc.year):
            days_in_year=366
        else:
            days_in_year=365
        for jday in range(tsutc.julday,days_in_year+1):
            result.append([tsutc.year,jday])
        for jday in range(1,teutc.julday+1):
            result.append([teutc.year,jday])
    return result
        
    
def run_by_days(df,
                start,
                end,
                dbname_or_handle,
                pad=100.0,
                channel_select="B*",
                maxdays=2,
                collection="wf_s3",
                auxkeys=["source_id"],
            )->list:
    """
    This function takes a (potentially large) DataFrame df of arrival 
    time data and returns a list of documents that can be used to 
    efficiently extract waveform data segments containing those arrival 
    times.   
    
    The documents returned each define one or more s3 objects that 
    are to be retrieved and cut up into the segments defined by 
    arrivals within the data range defined by the objects retrieved.  
    In most cases that will be on day file, but some time windows 
    will cross day boundaries.  Then multiple days will need to be loaded
    and merged before the windows are cut.  The output of this function
    signals that need by defining more than one s3 object in some 
    documents.  Specifically, the content of the key "s3objects" is a 
    list.  When that list has a length greater than 1 a merge is required.  
    
    This function uses the DataFrame api in combination with pymongo 
    bo build the output.  It assumes an index to the earthscope s3 
    archive already existss for the time period to be handled.  
    That index is expected to live in a collection defined by the 
    "collction" argument (default is "wf_s3").   
 
    The input DataFrame must contain the following required attributes:
      'time' - either a theoretical or measured arrival time used as a basis for 
               what segments are generated. The functio aims to extract a 
               window of size time+start-pad to time+end+pad for each row of df.
      'net' - SEED station net code
      'sta' - SEED station code
      
    Optional:   the keys defined by "auxkeys" are also expected to be in the 
      input DataFrame but will be silently ignored if they are not present.  
      The default expects "source_id" which is one way to provide a 
      cross reference to source data.  Something of that sort is 
      normally required to make the results easier to use since 
      arrivals by definition are associated with some source.  
      The idea is this function is generic, however, and it does not 
      need to be dogmatic about source data.  The main concept is to 
      extract event segments.  Use data in auxkeys as metadata that define 
      what those segments are.  

    A perspective on this function is it takes an input DataFrame with 
    the required metadata listed above and convert it to a new 
    DataFrame with the following attributes:
        's3objects' - list of s3 object name to be retrieved as an atomic operation
           Note when more than 1 reader needs to assume the pieces 
           need to be glued together.  Currently the list can be no longer 
           than 2.
        'net' - SEED net code
        'sta' - SEED station code
        'channel_select' - echo of the channel_select argument
           (used in s3 reader to select matching channel codes only)
        'arrivals' - list of dictionaries defining data to extract
        'start' - relative start time for windowing
        'end' - relative end time for windowing
        
    Note the 'arrivals' dictionaries each contain a 'time' attribute 
    that defines the arrival time from which a window is computed.  
    Any other data in tha dictionary is treated as auxiliary Metadata that 
    is to be copied to the output.  Note downstream uses of that aux 
    data must handle it carefully to make sure the contents do not contain 
    metadata that conflict with metadata used inside children of 
    BasicTimeSeries data (e.g. starttime).  Also note the start end 
    range will be modified from input by pad value. 

    """
    db = fetch_dbhandle(dbname_or_handle)
    df = df.sort_values("time")
    # Add two columns to the DataFrame that are used below to generate 
    # dayfiles to load to handle which arrivals.
    yrdaylists=list()
    yrdaystr=list()   # string version to be hashable - needed by groupby below
    for row in df.itertuples():
        ydlist=days_needed(row.time,start,end,pad=pad)
        if len(ydlist)>maxdays:
            # this should only happen if the user makes an input error for start and/or end
            message="Number of days for time={} and range={}-{} is too large\n".format(row.time,start,end)
            message+="Number of days this would try to return={} but maxdays={}".format(len(ydlist),maxdays)
            raise ValueError(message)
        ydstr = str(ydlist)
        yrdaylists.append(ydlist)
        yrdaystr.append(ydstr)
    df["yrdaylist"] = yrdaylists
    df["groupingkey"] = yrdaystr
    # sort and group by the hashable yrdaystr. We can't use the "yrdaylist" contents 
    # directly because list objects are not "hashable" as required for sorting and grouping
    df.sort_values(["groupingkey","net","sta"])
    grouped_df = df.groupby("groupingkey")
    result = list()   
    for key,gdf in grouped_df:
        #print(key)
        #print(gdf)
        # each grouped_df here has the same list of year-day pairs that need to be retrieved 
        # we can then fetch just the first of these to generate the db query to get object names
        ydlist = gdf["yrdaylist"].iloc[0]
        if len(ydlist)==1:
            query = {"year" : ydlist[0][0],
                     "jday" : ydlist[0][1],
                }
        elif len(ydlist)<=maxdays:
            orlist = list()
            for tup in ydlist:
                md = {"year" : tup[0], "jday" : tup[1]}
                orlist.append(md)
            query = {"$or" : orlist}
        else:
            raise ValueError("too many days or zero days")
        if db[collection].count_documents(query) == 0:
            print("Warning:   this query yielded no data:  ",query)
            print("Skipping data for ",key)
            continue
        cursor = db[collection].find(query)
        dfholdings = pd.DataFrame(cursor)
        # now we match rows of dfholdings with those of gdf and 
        # using net and sta as match keys.  We use DataFrame join 
        # for that operation - the method is called merge
        # the suffixes argument may not be necessary here but better than 
        # using defaults if this evolves to need it
        #print("DEBUG:  Size of dataframes being merged")
        #print("gdf size=",len(gdf)," dfholding size=",len(dfholdings))
        #print("gdf keys=",gdf.keys())
        #print("dfholdings keys=",dfholdings.keys())
        dfjoin = pd.merge(gdf,dfholdings,on=["net","sta"],suffixes=('_arrival','_db'))
        # now sort by net sta and group again 
        dfjoin=dfjoin.sort_values(["net","sta"])
        dbjgrp = dfjoin.groupby(["net","sta"])
        s3set = set()  # used to retain unique s3 object values in this loop
        for key2,dgf2 in dbjgrp:
            # with multiple key groupby key is a tuple 
            net = key2[0]
            sta = key2[1]
            s3set = set()  # needed to retain unique values only 
            stlist = list()
            etlist = list()
            arrivals=list()
            for row in dgf2.itertuples():
                a=dict()
                time = row.time
                a['time'] = time
                # in this context it is convenient to convert this to a dictionary 
                # to copy only requested keys and not the whole thing
                row_dict = row._asdict()
                for ak in auxkeys:
                    if ak in row_dict:
                        a[ak] = row_dict[ak]
                arrivals.append(a)
                stime = time + start - pad
                etime = time + end + pad
                stlist.append(stime)
                etlist.append(etime)
                s3set.add(row.s3key)
            s3object_list = list(s3set)
            doc = {"net" :  net,
                    "sta" : sta,
                    "channel_select" : channel_select,
                    "s3objects" : s3object_list,
                    "arrival" : arrivals,
                    "start" : stlist,
                    "end" : etlist,
                    }
            result.append(doc)
                
    return result
                
def has_live_data(ensemble)->bool:
    """
    Return True if an ensemble has any live data.   Assumes ensemble is either a TimeSeriesEnsemble 
    or Seismogram ensemble so the member attribute is iterable and has a live attribute.
    """
    for d in ensemble.member:
        if d.live:
            return True
    return False

async def get_object_with_iris_versions(s3_client,
                                  base_name,
                                  BUCKET,
                                  versions2try=["None","#1","#2","#3"],
                                  )->tuple:
    """
    At the present time the Earthscope s3 waveform archives use a relic
    version tag used in their server implementation to identify different
    versions of the same data.   They add names "#1", "#2", etc
    (unknown level is possible at this writing) to a base name.  e.g.
    a typical base name might be this:
        "miniseed/AZ/2010/060/MONP2.AZ.2010.060"
    which with a version tag would be a thing like this (for "#1"):
        "miniseed/AZ/2010/060/MONP2.AZ.2010.060#1"

    At the moment use of that versioning seems transient and sometimes you
    need it and sometimes you don't.   This function works around that
    problem try using the more lightweibht "head_object" method of the
    s3 client to try to find a valid name before it tries a full
    fetch with get_object.

    This function returns one of two things:
        1.  On success it returns a tuple with 0 being a valid obspy
            Stream object and 1 being an ErrorLogger object that may or
            may not be empty
        2.  If there is any failure it returns a [None,elog] tuple
            where elog is an ErrorLogger object with messages explaining
            what went wrong.

    The idea is this function should be totally bombproof.  If component 0
    of the return is None then  it failed but otherwise caller can assume
    it can use the obspy stream in component 0.

    :param s3_client:   s3 client to use for access.  An assumption is
      this function should NEVER be serialized in a parallel construct.
      Normal use may be inside a reader that itself may be run in parallel
      but that reader must have a worker copy of the s3 client it send to
      this function.
    :param base_name:  s3 object base name - without a "#" version appendage
    :param BUCKET:  s3 bucket name banded to s3_client.
    :param versions2try:  list of version tags to use - users should not
      alter this parameter.  It could have been done as an internal
      constant but use as a kwarg makes it clearer what all is checked
      before giving up.  Note the None is used to try fetching the object
      with no version tag - that is always done first.
    """
    elog = ErrorLogger()
    alg = "get_object_with_iris_versions"
    ntries = 0   # used to test if nothing worked
    version_found = "FAILED"
    for version in versions2try:
        ntries += 1
        if version is None:
            s3name = base_name
        else:
            s3name = base_name + version
        try:
            #print("DEBUG:  trying ",s3name)
            # this has less overhead to test for existence
            # intentionally throw the bits returned on the floor
            response = await s3_client.head_object(
                Bucket=BUCKET,
                Key=s3name,
                )
            version_found=version
            #print("Found key=",s3name)
            break
        except ClientError as e:
            #print("DEBUG:  error handler entered for s3name=",s3name)
            message = "Error trying to fetch object with name={}\n".format(s3name)
            if e.response['Error']['Code'] == '404':
                message += "object with that name does not exist"
                continue
            elif e.response['Error']['Code'] == '403':
                message += "object exists but access is restried and you cannot retrieve it"
                elog.log_error(alg,message,ErrorSeverity.Invalid)
                continue
            else:
                message += "Unknown error occurred - here is the message posted:\n{}".format(e)
                elog.log_error(alg,message,ErrorSeverity.Invalid)
                continue
    #print("DEBUG:  found version = ",version_found," will try key = ",s3name)
    if version_found == "FAILED":
        message = f"No valid version of object name {base_name} was found - cannot retieve these data"
        elog.log_error(alg,message,ErrorSeverity.Invalid)
        return [None,elog]
    recover_failed = False
    # python oddity requires this be initialized
    raws3data = None
    try:
        #print("DEBUG:  trying to fetch ",s3name)
        s3obj = await s3_client.get_object(Bucket=BUCKET, Key=s3name)
        async with s3obj["Body"] as stream:
            raws3data = await stream.read()
    except ClientError as e:
        message = "s3_client.get_object method failed fetching object name={}".format(s3name)
	# try to recover if the name has version number
        testkey = s3name.split("#")
        if len(testkey)>1:
            try:
                print("Trying to recover by fetching ",testkey[0])
                s3obj = await s3_client.get_object(Bucket=BUCKET, Key=testkey[0])
                async with s3obj["Body"] as stream:
                    raws3data = await stream.read()
            except ClientError as e:
                #print("DEBUG:  Entered error handler for get_object section")
                recover_failed = True
                message += "Recovery failed with alternative key = {}\n".format(testkey[0])
                message += "Exception message from handler follows:\n"
                # Generate a descriptive message to define problem
                # note this list comes from Gemini - not easy to know if list is up to date
                error_code = e.response['Error']['Code']
        
                if error_code == 'NoSuchKey':
                    message += f"The object {s3name} does not exist in bucket {BUCKET}."
                elif error_code == 'AccessDenied':
                    message += f"Access denied to {s3name}. Check IAM policies/KMS keys."
                elif error_code in ['NoSuchBucket', 'AllAccessDisabled']:
                    message += f"Fatal S3 Configuration Error: {error_code}"
                else:
                    message += f"Unexpected AWS ClientError ({error_code}): {e}"
                elog.log_error(alg,message,ErrorSeverity.Invalid)
                return [None,elog]

    except BotoCoreError as e:
        message += f"Low-level BotoCore error occurred: {e}"
        elog.log_error(alg,message,ErrorSeverity.Fatal)
        return [None,elog]
    except Exception as e:
        message += f"Unexpected exception was thrown with this meesage:\n{e}"
        elog.log_error(alg,message,ErrorSeverity.Fatal)
        return [None,elog]
    finally:
        # Convert to an obspy stream and return that
        # if it fails return a None and different elog message
        try:
            strm = obspy.read(io.BytesIO(raws3data),format="mseed")
            #print(strm)
            return [strm,elog]
        except Exception as e:
            message = f"obspy.read failed decoding data retrieved from s3 object name={s3name}\n"
            message += f"Error message posted:\n{e}"
            elog.log_error(alg,message,ErrorSeverity.Invalid)
            return [None,elog]

async def s3_segmenter(doc,
                       session=None,
                       bucket=BUCKET,
                       detrend_type="None",
                       short_segment_handling="kill",
                     )->TimeSeriesEnsemble:
    """
    Low level function to take content of doc, which is assumed to 
    be a componet of the output of run_by_days, and return a 
    TimeSeries ensemble of waveform segments the doc defines. 
    """
    IMMUTABLE_METADATA = ["starttime","endtime","delta","npts","utc_convertible","time_standard"]
    # defined here so if the strm is empty we return a default 
    # constructed ensemble with error log content
    result = TimeSeriesEnsemble()
    s3_client=None
    try:
        #print("Creating s3 client")
        if session is None:
            #print("Trying to create s3_client in parallel mode")
            s3_client=await fetch_s3_client()
            #print("success")
        else:
            s3_client=await fetch_s3_client(session)
        s3objects=doc["s3objects"]
        for s3key in s3objects:
            # this function should never throw an exception but return a None if 
            # if fails strmread
            print("Trying to fetch data s3 object with key=",s3key)
            strm,elog = await get_object_with_iris_versions(s3_client,s3key,BUCKET)
            if strm is None:
                print("DEBUG:  empty stream - returning empty ensemble")
                bad_result = TimeSeriesEnsemble()
                bad_result.elog = elog
                return bad_result
 
        print("size of stream from obspy reader=",len(strm))
        channel_select = doc["channel_select"]
        if len(channel_select)>0:
            strm=strm.select(channel=channel_select)
        print("stream size after select=",len(strm))
        if len(strm)>0:
            if detrend_type != "None":
                strm.detrend(detrend_type)
            # this is essential or merge will not work
            # it works by comparing successive Trace objects
            strm.sort()
            strm.merge()
            print("Stream size sent to ensemble converter = ",len(strm))
            alldata = Stream2TimeSeriesEnsemble(strm)
            del strm
            # elog contains error messages posted by get_object_with_iris_versions 
            # always save them to sort out data store problems by sifting through elogs
            result.elog += elog
            # workaround for bug github issue 710
            if has_live_data(alldata):
                alldata.set_live()
            else:
                #print("Ensemble with ",len(alldata.member)," members has no data marked live - returning empty result")
                return result
            print("Number of live data in alldata=",number_live(alldata))
            stlist = doc["start"]
            etlist = doc["end"]
            arrival_auxdata = doc["arrival"]
            # We assume the three items indexed here have the same length 
            for i in range(len(stlist)):
                stime = stlist[i]
                etime = etlist[i]
                arrival_doc = arrival_auxdata[i]
                #print("Running window data")
                ens = WindowData(alldata,stime,etime,short_segment_handling=short_segment_handling)
                #print("window output state=",ens.live)
                #print("Size of output=",len(ens.member))
                for i in range(len(ens.member)):
                    d = ens.member[i]
                    for k in arrival_doc:
                        if k not in IMMUTABLE_METADATA:
                            if k=="time":
                                d["arrival_time"] = arrival_doc["time"]
                            else:
                                d[k] = arrival_doc[k]
                    ens.member[i] = d
                #print("Size ens after metadata copy=",len(ens.member))
                for d in ens.member:
                    result.member.append(d)
                #print("Size of result = ",len(result.member))
            if has_live_data(result)>0:
                #print("Setting result live")
                print("Returning ensemble with ",number_live(ens)," live members")
                result.set_live()
    except Exception as e:
        message="basic_s3_segmenter failed with when the following exception was thrown:\n"
        message += str(e)
        result.elog.log_error("basic_s3_segmente",message,ErrorSeverity.Invalid)
    #print("Debug - returning result with state=",result.live)
    return result

def make_sync(async_func):
    """
    Takes an async function and wraps it inside a standard synchronous function.
    """
    def sync_wrapper(*args, **kwargs):
        # Drive the async function to completion using an event loop
        return asyncio.run(async_func(*args, **kwargs))
        
    return sync_wrapper
