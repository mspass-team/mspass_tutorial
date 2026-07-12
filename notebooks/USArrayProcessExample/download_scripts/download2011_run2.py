import time
from rfteledownload import read_centers
from rfteledownload import get_catalog
from rfteledownload import download_events
from obspy import read_events

centers=read_centers()
yr=2011
t0=time.time()
#print("Running get_catalog")
#cat=get_catalog(centers,year=yr)
fname=("events_%d.xml" % yr)
#cat.write(fname,format='QUAKEML')
#t1=time.time()
#print("Elapsed time for to fetch catalog=",t1-t0)
cat=read_events(fname,format='QUAKEML')
print("Running download_events from id 904")
t1=time.time()
download_events(cat,yeartag=str(yr),firstid=904)
t2=time.time()
print("Elapsed time to download events=",t2-t1)
