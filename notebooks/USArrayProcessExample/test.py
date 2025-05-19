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
n=db.wf_TimeSeries.count_documents({})
print("Number of TimeSeries in {}: {}".format(dbname, n))
n=db.wf_Seismogram.count_documents({})
print("Number of Seismograms in {}: {}".format(dbname, n))