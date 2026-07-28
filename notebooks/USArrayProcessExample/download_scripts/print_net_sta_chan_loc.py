import sys
from obspy import Inventory
from obspy import read_inventory
def chanlist2tuples(chanlist):
    result=[]
    for x in chanlist:
        z=x.split('.')
        net=z[0]
        sta=z[1]
        loc=z[2]
        chan=z[3]
        # Although slightly irrational for this program I make the order
        # of output net, sta, chan, loc because that order is locked in my brain
        # as an old antelope user
        lnetsta=(net,sta,chan,loc)
        result.append(tuple(lnetsta))
    return result

stadir=''
args=sys.argv
if(len(args)!=2):
    print('Usage Error:\npython print_net_sta_chan_loc.py stadir')
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
    channels=x['channels']
    # The list in channels has names in the form net.sta.loc.chan
    # This function converts these to a list of tuples in form (net,sta,chan,loc)
    chantuples=chanlist2tuples(channels)
    for x in chantuples:
        if(len(x[3])==0):
            print(x[0],x[1],x[2],'NULL')
        else:
            print(x[0],x[1],x[2],x[3])
except:
    print('something threw an exception')
