import time
from obspy import UTCDateTime
from obspy.clients.fdsn.mass_downloader import RectangularDomain, \
    Restrictions, MassDownloader
from obspy.clients.fdsn import Client


def read_centers(file='year_centers.txt'):
    """
    Read text file of array centers.  three numbers per
    line:  year of coverage, longitude, latitude.
    Returns dict of lon,lat pairs (list) indexed by integer
    year.
    """
    centers={}
    # More elegant method is to use with, but not necessary
    # for this private program.
    fh=open(file,mode='r')
    for line in fh:
        words=line.split()
        yr=int(words[0])
        lon=words[1]
        lat=words[2]
        centers[yr]=[lon,lat]
    return centers

def get_catalog(centers, year=2005, minmag=4.9):
    """
    Fetch year of data for usarray using coordinates for 
    center indexed by year and rf distance range of 25 to 
    95 degree from array center.  Range is slightly larger 
    than normal due to large array aperture of usarray.   
    May get some extras, but part of this exercise is 
    filtering problem data.
    
    :param centers:   dict of array centers keyed by year
    :param year:  year to use to load events
    :param minmag:  lower limit on magnitude (no upper limit)
    
    :rtype: obspy catalog object
    """
    client=Client("IRIS")
    tstart=UTCDateTime(year,1,1,0,0)
    tend=UTCDateTime(year+1,1,1,0,0)
    coords=centers[year]
    lon0=coords[0]
    lat0=coords[1]
    cat=client.get_events(starttime=tstart,endtime=tend,
                          latitude=lat0,longitude=lon0,
                          minradius=25.0,maxradius=95.0,
                          minmagnitude=minmag)
    return cat

def download_events(cat,yeartag="2005",firstid=1):
    """
    Download events to lower 48 with MassDownloader using
    events in obspy Catalog object cat.  Parameters for 
    station definitions are hard wired 
    """
    wf_storage=yeartag + "/{starttime}/{network}_{station}_{location}.mseed"
    site_storage="site_"+yeartag+".dir"
    mdl = MassDownloader()
    domain = RectangularDomain(minlatitude=20.0,maxlatitude=54.0,
        minlongitude=-135, maxlongitude=-55)
    count_evid=1
    for event in cat:
        if(count_evid>=firstid):
            t0=time.time()
            o=event.preferred_origin()
            # Time attribute is already a UTCDateTime object
            origin_time=o.time
            restrictions = Restrictions (
             starttime=origin_time,
             endtime=origin_time + 3600.0,
             reject_channels_with_gaps=True,
             minimum_length=0.95,
             minimum_interstation_distance_in_m=100.0,
             channel_priorities=["BH[ZNE12]"],
             location_priorities=["","00","10","01"])
            
            wf_storage=("%s/event%d" % (yeartag,count_evid))
            mdl.download(domain,restrictions,
                mseed_storage=wf_storage,
                stationxml_storage=site_storage)
            dt=time.time()-t0
            print("Event ",count_evid," download time (s)=",dt)
        count_evid += 1

centers=read_centers()
yr=2012
t0=time.time()
print("Running get_catalog")
cat=get_catalog(centers,year=yr)
fname=("events_%d.xml" % yr)
cat.write(fname,format='QUAKEML')
t1=time.time()
print("Elapsed time for to fetch catalog=",t1-t0)
print("Running download_events")
t1=time.time()
download_events(cat,yeartag=str(yr))
t2=time.time()
print("Elapsed time to download events=",t2-t1)
