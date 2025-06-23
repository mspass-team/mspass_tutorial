import io 
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
import pandas as pd
import obspy
from mspasspy.db.database import Database
import mspasspy.client as msc
from mspasspy.ccore.seismic import TimeSeriesEnsemble
from mspasspy.util.converter import Stream2TimeSeriesEnsemble
def get_s3_index_keys(index,bucket="scedc-pds",s3client=None)->list:
    """
    Returns a set of s3 keys for index files used by SCEDC to provide a 
    hierarchic search capability.   Although the bucket name is optional
    this may not work for any other data center as the structure of the
    SCEDC archive defines that form of the key.  i.e. the files that 
    define the index s3 objects that act like files stored in two 
    directories called "continuous_waveforms/index" and 
    "earthquake_catalogs/index".  
    
    :param index:   name of the index.  Special to this function 
      Must be either "wf" or "catalog".
    :param bucket:  s3 bucket name. Default "scedc-pds" should be changed 
      only if you are sure the alternative archive has the same structure 
      as the scedc archive - unlikely.   This parameter should be 
      thought of as more-or-less a constant.
    :param s3client:  client to use for retrieving the index.  
      If None (default) an anonymous client will be created by the 
      function and destroyed when it exits.   Set for efficiency or if 
      you need to use a client with credentials.  
    """
    if s3client is None:
        s3client = boto3.client('s3',config=Config(signature_version=UNSIGNED))
    if index=="wf":
        prefix = "continuous_waveforms/index/"
    elif index=="catalog":
        prefix = "earthquake_catalogs/index/"
    else:
        message = "get_s3_index_keys:  illegal value for index={}".format(index)
        raise ValueError(message)
    # Derived from google ai suggestion the while loop is needed for 
    # large files - I think the object may be truncated otherwise
    resp = s3client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys=[]
    if 'Contents' in resp:
        for obj in resp['Contents']:
            keys.append(obj['Key'])

    while resp.get('IsTruncated'):
        continuation_key = resp.get('NextContinuationToken')
        resp = s3client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            ContinuationToken=continuation_key
        )
        if 'Contents' in resp:
            for obj in resp['Contents']:
                keys.append(obj['Key'])

    return keys

    
def wfkey2doc(wfkey):
    """
    Converts a SCEDC continuous waveform key for s3 stores to a document/dictionary 
    that can be saved in MongoDB
    """
    doc={"s3key" : wfkey}
    doc["index_type"]="continuous_waveforms"
    pathlist=wfkey.split("/")
    doc['format'] = pathlist[2]
    yrstr = pathlist[3]
    yr = yrstr.split("=")
    doc["year"] = int(yr[1])
    # obnoxious format - need to split things like "year_day=2004_004" to exract 004
    yrday=pathlist[4]
    
    ydl=yrday.split("=")
    yd_str = ydl[1]
    doc["yrday"]=yd_str   # convenient to save this magic string for key generation
    yd = ydl[1].split('_')
    doc["day"] = int(yd[1])
    doc["filename"] = pathlist[5]
    return doc

def catkey2doc(catkey):
    """
    Converts an SCEDC catalog index key for s3 stores to a document/dictionary
    that can be stored in MongoDB.  Catalog files are yearly so the 
    structure of this quite different from the waveform s3 keys.
    """
    doc={"s3key" : catkey}
    doc["index_type"]="earthquake_catalogs"
    pathlist=catkey.split("/")
    doc['format'] = pathlist[2]
    yrstr = pathlist[3]
    yr = yrstr.split("=")
    doc["year"] = int(yr[1])
    doc["filename"] = pathlist[4]
    return doc

def s3_index_reader(doc,bucket="scedc-pds")->pd.DataFrame:
    form = doc["format"]
    if form in ["csv","parquet"]:
        s3client = boto3.client('s3',config=Config(signature_version=UNSIGNED))
        s3key = doc["s3key"]
        obj = s3client.get_object(Bucket=bucket, Key=s3key)
        buffer = io.BytesIO()
        buffer.write(obj['Body'].read()) # Read the entire object data into the buffer
        buffer.seek(0) # Reset the buffer's position to the beginning

        if form=="csv":
            return pd.read_csv(buffer)
        else:
            # with this logic this is like elif parquet
            return pd.read_parquet(buffer)
        
def make_day_index(index_doc)->list:
    """
    Uses a document retrieved from the MongoDB collection "SCEDC_s3_index"
    (assumed conent of index_doc of arg0) and queries s3 to retrieve all 
    s3 objects for the day that document defines (year and day attributes).
    It will throw an exception if the index document "index_type" is 
    not "continuous_waveforms".  It also demands the format be parquet 
    as the csv files may lack headers with attribute names.

    Result is returned as a list of python dictionaries (documents) 
    that can be used directly for parallel processing or saved to 
    the MongoDB database.
    """
    if index_doc["index_type"] != "continuous_waveforms":
        message = "make_day_index:  document value for index_type={} is not allowed.  Must be continuous_waveforms".format(index_doc["index_type"])
        raise ValueError(message)
    if index_doc["format"] != "parquet": 
        message = "make_day_index:  document value for format={} is not allowed.  Must be parquet".format(index_doc["format"])
        raise ValueError(message)
    # next load the parquet file
    df = s3_index_reader(index_doc)
    # we need these to construc the s3 key 
    # that "key" is in the format of a unix file path with a format defined in the SCEDC documentation
    # paths are of the form:  continuous_waveforms/year/year_day/filename 
    year = index_doc["year"]
    yrday = index_doc["yrday"]
    fname = index_doc["filename"]
    
    doclist = df.to_dict(orient="records")
    for doc in doclist:
        fname = doc["ms_filename"]
        s3key = "continuous_waveforms/{}/{}/{}".format(year,yrday,fname)
        doc["s3key"]=s3key
    return doclist

def s3_mseed_reader(doc,s3client=None,bucket="scedc-pds")->TimeSeriesEnsemble:
    if s3client is None:
        s3client = boto3.client('s3',config=Config(signature_version=UNSIGNED))
    s3key=doc["s3key"]
    try:
        obj = s3client.get_object(Bucket=bucket,Key=s3key)
        mseed_content=obj["Body"].read()
        st = obspy.read(io.BytesIO(mseed_content),format="mseed")
        ens = Stream2TimeSeriesEnsemble(st)
        if len(ens.member)>1:
            print("Warning - file was segmented")
            print("Number of segments=",len(ens.member))
        return ens
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("Error fetching data with s3 key=",s3key)
            print("The s3 client say the specified object does not exist.")
        else:
            # Handle other potential ClientErrors
            print(f"An unexpected error occurred runnign get_object: {e}")
        return None
