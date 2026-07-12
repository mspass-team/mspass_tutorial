import sys
from obspy import Inventory
from obspy import read_inventory
# Needed below to sort by sta as second element of tuples
def GetKeyFromTuple(item):
    return item[1]
def stalist2tuples(stalist):
    result=[]
    for x in stalist:
        # This ridiculous double call to split is needed because a typical
        # element of statlis looks like this:
        # AZ.BZN (Buzz Northerns Place, Anza, CA, USA)
        z=x.split('.')
        net=z[0]
        zz=z[1].split(' ')
        sta=zz[0]
        # Although slightly irrational for this program I keep the order
        # net sta because that order is locked in my brain and most
        # seismologists
        lnetsta=(net,sta)
        result.append(tuple(lnetsta))
    return result

stadir=''
args=sys.argv
if(len(args)!=2):
    print('Usage Error:\npython find_duplicate_net_sta stadir')
    exit(2)
try:
    # This assumes structure created by bulk_download where stations are
    # put in files in a directory with one station per file - why the * wildcard
    # is needed
    xmldir=args[1]
    xmlfiles=xmldir+"/"+"*.xml"
    inv=read_inventory(xmlfiles,format='STATIONXML')
    # This returns an absurd structure in my opinion. For this we need only
    # the stations data
    x=inv.get_contents()
    stations=x['stations']
    # The list in stations has names in the form net.sta
    # This function converts these to a list of tuples in form (net,sta)
    # Thes can  sorted with the station key using GetKeyFromTuple
    stup=stalist2tuples(stations)
    stupsorted=sorted(stup,key=GetKeyFromTuple)
    laststa=''
    lastnet=''
    last_n=0
    print('net sta values read from directory=',xmldir)
    # this is not a memory efficient algorithm, but is a simpler algorithm
    # than a line by line test for duplicates I've always found easy to
    # mess up.  This is pythonic pushing index positions to a list for
    # any duplicate station
    dups=dict()
    duplist=list()
    n=0
    ndups=0
    for x in stupsorted:
        net=x[0]
        sta=x[1]
        if(n==0):
            laststa=sta
            lastnet=net
            n=1
            continue
        #print(n,net,sta)
        if(sta==laststa):
            if(ndups==0):
                duplist.clear()
                duplist.append(lastnet)
                duplist.append(net)
                dups[sta]=duplist
            else:
                y=dups[sta]
                y.append(net)
                dups[sta]=y
            #print(ndups,sta,dups[sta])
        else:
            ndups=0
        n+=1
        laststa=sta
        lastnet=net
    print('Duplicates:')
    for sta in dups:
        x=dups[sta]
        for net in x:
            print(net,sta)
except RuntimeError as err:
    print('err')
