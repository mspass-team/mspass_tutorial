#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module is something around verions 3 or 4 of a set of utility
functions for working with s3 on geolab.   Their purpose is to standardize
componnts needed in workflows to extract waveform segments from the
continuous archives.

Created on Sat Aug 22 06:04:50 2026

@author: pavlis
"""
import io
from contextlib import closing
import obspy
from obspy import UTCDateTime
import pandas as pd
import calendar
from mspasspy.util.db_utils import fetch_dbhandle
from mspasspy.ccore.seismic import TimeSeriesEnsemble
from mspasspy.util.converter import Stream2TimeSeriesEnsemble
from mspasspy.util.seismic import has_live_data
from mspasspy.algorithms.window import WindowData
from mspasspy.algorithms.signals import detrend
from mspasspy.ccore.utility import ErrorLogger, ErrorSeverity
from botocore.exceptions import ClientError
from s3_worker_plugin import fetch_s3_client


def fetch_net_day_list(
    s3_client,
    net,
    year,
    day,
    bucket="earthscope-mseed-res-na3mtd4fq5kz7pntcyr1uh46use2a--ol-s3",
    server_data_key="server_data",
) -> tuple:
    """
    Fetch the raw list of object names from s3 for network net for year and julian day day.
    Returns a tuple/array of length 2.  Comp 0 is a list of s3 object names for that
    net, year, day combination.  Comp 1 is a dictionary returned by aws of the attribute
    they call "HPPTHeader".   The main use of that is to define the server and aws region
    from which the data can be fetched.   That is contant for this tutorial using Earthscope
    data only, but it is useful to include it as access from multiple servers would need that data.
    Default bucket argument is that for earthscope s3 miniseed archives.
    """
    base_prefix = "miniseed"
    prefix = f"{base_prefix}/{net}/{year}/{day:03d}/"
    keys = set()
    server_data = {}
    # One ListObjectsV2 response is capped at 1,000 keys.  Propagate listing
    # failures rather than turning an authentication outage into an empty day.
    for response in s3_client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix, Delimiter="/"
    ):
        keys.update(obj["Key"].split("#")[0] for obj in response.get("Contents", []))
        server_data = response.get("ResponseMetadata", {})
    return [sorted(keys), {server_data_key: server_data}]


def objlist2doclist(objlist, auxmd=None):
    """
    Splits s3 object names into dictionary entries to create a document to be saved in MongoDB.
    Use dictionary like object *auxmd* to load additional, constant metadata to each doc returned.
    """
    doclist = list()
    for objname in objlist:
        doc = dict()
        doc["s3key"] = objname
        s = objname.split("/")
        doc["net"] = s[1]
        yr = int(s[2])
        doc["year"] = yr
        jday = int(s[3])
        doc["jday"] = jday
        starttime = UTCDateTime(year=yr, julday=jday)
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


def days_needed(t0, start, end, pad=100.0) -> list:
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
    if end < start:
        message = "days_needed:  start time={} is less than end time={}".format(
            start, end
        )
        raise ValueError(message)
    tsutc = UTCDateTime(t0 + start - pad)
    teutc = UTCDateTime(t0 + end + pad)
    result = list()
    if tsutc.year == teutc.year:
        yr = tsutc.year
        for jday in range(tsutc.julday, teutc.julday + 1):
            result.append([yr, jday])
    else:
        # complexity required to cross year boundary
        # first a sanity check
        if (teutc.year - tsutc.year) != 1:
            message = "days_needed:  received irrational input.\n"
            message += "Time window of {} -> {} spans multiple years\n".format(
                tsutc, teutc
            )
            message += "Irrational input as the volume of data to download is too large"
            raise ValueError(message)
        if calendar.isleap(tsutc.year):
            days_in_year = 366
        else:
            days_in_year = 365
        for jday in range(tsutc.julday, days_in_year + 1):
            result.append([tsutc.year, jday])
        for jday in range(1, teutc.julday + 1):
            result.append([teutc.year, jday])
    return result


def build_day_processing_blocks(
    df,
    start,
    end,
    dbname_or_handle,
    pad=100.0,
    channel_select="B*",
    maxdays=2,
    collection="wf_s3",
    auxkeys=["source_id"],
) -> list:
    """
    This function takes a (potentially large) DataFrame df of arrival
    time data and returns a grouped list of documents that can be used to
    efficiently extract waveform data segments containing those arrival
    times.

    The data structure returned is a bit complex because it provides a
    clean, if not obvious, way to work efficiently within constraints of
    GeoLab and the current implementation of the MsPASS Database.
    That is, the only scalable method of output on GeoLab at present is to
    push data to an output s3 bucket on their scratch bucket.   The only
    we we have a present to do that is to serialize data with pickle and
    push the bits to s3 objects.   The idea of the output of this function
    is to organize the processing to produce ensembles that are essentially
    segmented day files.   i.e. the groupings define all data that have a
    starttime on a particular day.   A subtle element of this approach is
    that some events will be split across day volumes in the rare situation
    where the set of arrival times for that event cross a day boundary.

    To handle that complexity the output is a list of lists.   The
    outer list of the return is of the form:
        [data_list, error_list]
    The content of "data_list" is itself a list of pairs (2 element array/list)
    with conetent:
        [yearday_string,document_list]
    where "yearday_string" is a like it sound (e.g. 2004122 for julian
    day 122 of 2004).  document_list is a list of dictionaries that are used
    downstream in parallel by a function called `get_s3_segments`.

    The complexity of the outer list component I called `error_list` above is
    needed as a necessary evil as an error return.   Because the input to
    this function is normally huge the list of failures can also be huge.
    Simply printing every failure can overwhelm the jupyter server when this
    is run on GeoLab (the expectation).   The list is a list of MongoDB
    queries that yield no data.  The caller should handle it appropriately.
    The expectation at present is to save the content as a pickle file
    in the run directory for later inspection.

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
    the required metadata listed above and convert to an input that
    can be used to implement processing by day groups.   The day groups
    provide a reasonably managable size package to push data as pickled
    objects to an output s3 bucket.   The primary content of each document
    in the output structure is:

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

    As noted any valid data match key defined by "auxdata" will also be
    posted to those documents.

    Finally, note the 'arrivals' dictionaries each contain a 'time' attribute
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
    yrdaylists = list()
    yrdaystr = list()  # string version to be hashable - needed by groupby below
    for row in df.itertuples():
        ydlist = days_needed(row.time, start, end, pad=pad)
        if len(ydlist) > maxdays:
            # this should only happen if the user makes an input error for start and/or end
            message = (
                "Number of days for time={} and range={}-{} is too large\n".format(
                    row.time, start, end
                )
            )
            message += (
                "Number of days this would try to return={} but maxdays={}".format(
                    len(ydlist), maxdays
                )
            )
            raise ValueError(message)
        # ydstr = f"{ydlist[0]}{ydlist[1]}"
        # ydstr = str(ydlist)
        ydstr = f"{ydlist[0][0]}{ydlist[0][1]}"
        # print(ydlist[0][0])
        # print(ydlist[0][1])
        yrdaylists.append(ydlist)
        yrdaystr.append(ydstr)
    df["yrdaylist"] = yrdaylists
    df["yearday"] = yrdaystr

    result = []
    fail_list = []
    for _, gdf in df.groupby("yearday", sort=False):
        # The first window need not span all the days used by later windows.
        required_days = sorted(
            {tuple(day) for days in gdf["yrdaylist"] for day in days}
        )
        query = {"$or": [{"year": yr, "jday": day} for yr, day in required_days]}
        holdings = {}
        for holding in db[collection].find(query):
            holdings.setdefault((holding["net"], holding["sta"]), []).append(holding)
        if not holdings:
            fail_list.append(query)
            continue
        first_day = gdf["yrdaylist"].iloc[0][0]
        jday_tag = f"{first_day[0]}_{first_day[1]}"
        daylist = []
        for (net, sta), station_rows in gdf.groupby(["net", "sta"]):
            objects = set()
            arrivals, starts, ends = [], [], []
            available = holdings.get((net, sta), [])
            for row in station_rows.to_dict("records"):
                days = {tuple(day) for day in row["yrdaylist"]}
                matching = [h for h in available if (h["year"], h["jday"]) in days]
                if not matching:
                    fail_list.append(
                        {
                            "net": net,
                            "sta": sta,
                            "$or": [
                                {"year": yr, "jday": day} for yr, day in sorted(days)
                            ],
                        }
                    )
                    continue
                # An arrival appears once, even when it needs two day objects.
                # A DataFrame join with the holdings would multiply arrivals.
                arrival = {"time": row["time"]}
                arrival.update({key: row[key] for key in auxkeys if key in row})
                arrivals.append(arrival)
                starts.append(row["time"] + start - pad)
                ends.append(row["time"] + end + pad)
                objects.update(h["s3key"] for h in matching)
            if arrivals:
                daylist.append(
                    {
                        "net": net,
                        "sta": sta,
                        "channel_select": channel_select,
                        "s3objects": sorted(objects),
                        "arrival": arrivals,
                        "start": starts,
                        "end": ends,
                        "jday_tag": jday_tag,
                    }
                )
        if daylist:
            result.append([jday_tag, daylist])
    return [result, fail_list]


def get_s3_segments(
    doc,
    session=None,
    bucket="earthscope-mseed-res-na3mtd4fq5kz7pntcyr1uh46use2a--ol-s3",
    detrend_type="None",
    short_segment_handling="kill",
    jday_tag_key="jday_tag",
) -> TimeSeriesEnsemble:
    """
    Low level function to take content of doc, which is assumed to
    be a componet of the output of run_by_days, and return a
    TimeSeries ensemble of waveform segments the doc defines.
    """
    alg = "get_s3_segments"
    IMMUTABLE_METADATA = [
        "starttime",
        "endtime",
        "delta",
        "npts",
        "utc_convertible",
        "time_standard",
    ]
    # defined here so if the strm is empty we return a default
    # constructed ensemble with error log content
    result = TimeSeriesEnsemble()
    s3_client = None
    # Always add the bucket name to the doc so it will be saved for both success and failure
    doc["Bucket"] = bucket
    # always put this in the ensemble metadata so there is always a way to identify a problem child
    result["s3_request_document"] = doc
    # Only serial calls create an owned client; a worker's plugin keeps its
    # client open for subsequent tasks.
    s3_client = fetch_s3_client() if session is None else fetch_s3_client(session)
    try:
        s3objects = doc["s3objects"]
        if len(s3objects) == 0:
            message = f"Request document had no entries in the list defined by the s3objects key"
            result.elog.log_error(alg, message, ErrorSeverity.Invalid)
            return result
        strm = None
        for s3key in s3objects:
            # Missing/undecodable data return None; infrastructure errors raise.
            # print("Debug:  trying to fetch s3key=",s3key)
            stread, elog = get_object_with_iris_versions(
                s3_client, s3key, bucket=bucket
            )
            if stread is None:
                result.elog = elog
                message = f"No data retrieved for request with s3object={s3objects}"
                result.elog.log_error(alg, message, ErrorSeverity.Invalid)
                # temporary for debugging
                # print(bad_result.elog.get_error_log())
                return result
            else:
                if strm is None:
                    strm = stread
                else:
                    strm += stread
                del stread

        try:
            channel_select = doc["channel_select"]
            if len(channel_select) > 0:
                strm = strm.select(channel=channel_select)
            # print("stream of size after select=",len(strm))
            if len(strm) > 0:
                # This handles a rare data problem where the same seed net code
                # will have multiple sample rate data.   Happens because SEED
                # only specifies a range for a a channel code
                rates = set(tr.stats.sampling_rate for tr in strm)
                # strm.sort()
                if len(rates) == 1:
                    strm.merge()
                    alldata = Stream2TimeSeriesEnsemble(strm)
                    del strm
                else:
                    # should not need to handle case with len 0
                    # here we split and merge pieces
                    merged_streams = obspy.Stream()
                    for rate in rates:
                        sub_st = strm.select(sampling_rate=rate).copy()
                        sub_st.merge(method=1)
                        merged_streams += sub_st
                    del sub_st
                    alldata = Stream2TimeSeriesEnsemble(merged_streams)
                    del strm
                    del merged_streams

                # elog contains error messages posted by get_object_with_iris_versions
                # always save them to sort out data store problems by sifting through elogs
                result.elog += elog
                if detrend_type not in (None, "None"):
                    alldata = detrend(alldata, type=detrend_type)
                # workaround for bug github issue 710
                if alldata.dead():
                    if has_live_data(alldata):
                        alldata.set_live()
                    else:
                        # print("Ensemble with ",len(alldata.member)," members has no data marked live - returning empty result")
                        result.elog.log_error(
                            alg,
                            "converted TimeSeriesEnsemble following detrend has no live data",
                            ErrorSeverity.Invalid,
                        )
                        return result
                stlist = doc["start"]
                etlist = doc["end"]
                arrival_auxdata = doc["arrival"]
                # We assume the three items indexed here have the same length
                for i in range(len(stlist)):
                    stime = stlist[i]
                    etime = etlist[i]
                    arrival_doc = arrival_auxdata[i]
                    # print("Running window data")
                    ens = WindowData(
                        alldata,
                        stime,
                        etime,
                        short_segment_handling=short_segment_handling,
                    )
                    # print("window output state=",ens.live)
                    # print("Size of output=",len(ens.member))
                    for i in range(len(ens.member)):
                        d = ens.member[i]
                        for k in arrival_doc:
                            if k not in IMMUTABLE_METADATA:
                                if k == "time":
                                    d["arrival_time"] = arrival_doc["time"]
                                else:
                                    d[k] = arrival_doc[k]
                        ens.member[i] = d
                    # print("Size ens after metadata copy=",len(ens.member))
                    for d in ens.member:
                        result.member.append(d)
                    # print("Size of result = ",len(result.member))
                if has_live_data(result) > 0:
                    # print("Setting result live")
                    result.set_live()
                    if jday_tag_key in doc:
                        result[jday_tag_key] = doc[jday_tag_key]
                    else:
                        message = f"No value found for key={jday_tag_key} - may cause problems downstream"
                        result.elog.log_error(alg, message, ErrorSeverity.Complaint)
                else:
                    result.kill()  # not necessary but sure kill make this more robust
                    message = "No live data extracted by windowing loop of this code"
                    result.elog.log_error(alg, message, ErrorSeverity.Invalid)
                del alldata
            else:
                result.elog = elog
                message = f"No data satisfied Stream.select({channel_select})"
                result.elog.log_error(alg, message, ErrorSeverity.Invalid)
        except MemoryError:
            raise
        except Exception as e:
            message = (
                "get_s3_segments failed with when the following exception was thrown:\n"
            )
            message += str(e)
            result.elog.log_error(alg, message, ErrorSeverity.Invalid)
        return result
    finally:
        if session is not None:
            s3_client.close()


def get_object_with_iris_versions(
    s3_client,
    base_name,
    bucket="earthscope-mseed-res-na3mtd4fq5kz7pntcyr1uh46use2a--ol-s3",
    maxversion=10,
):
    """Read a base object or its legacy #N version.

    Try the base name, then #maxversion down to #1.  Only a definite missing
    object (404/NoSuchKey/NotFound) permits fallback.  HEAD's generic 403 cannot
    establish whether an object exists.  Authentication, transport, body-read,
    and other infrastructure failures retain their original exception and
    stop the workflow.  Missing/undecodable data return [None, elog].
    """
    elog = ErrorLogger()
    alg = "get_object_with_iris_versions"
    names = [base_name] + [
        f"{base_name}#{version}" for version in range(maxversion, 0, -1)
    ]
    for name in names:
        try:
            s3_client.head_object(Bucket=bucket, Key=name)
        except ClientError as error:
            if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                continue
            raise
        response = s3_client.get_object(Bucket=bucket, Key=name)
        with closing(response["Body"]) as body:
            data = body.read()
        try:
            with io.BytesIO(data) as buffer:
                stream = obspy.read(buffer, format="mseed")
            return [stream, elog]
        except Exception as error:
            elog.log_error(
                alg,
                f"Cannot decode miniSEED object {name}: {error}",
                ErrorSeverity.Invalid,
            )
            return [None, elog]
    elog.log_error(
        alg,
        f"No valid version of object {base_name} was found",
        ErrorSeverity.Invalid,
    )
    return [None, elog]
