#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run script for CNRDecon afer preprocessing compleed.

Created on Fri Feb 28 08:31:33 2025

@author: pavlis
"""
from mspasspy.ccore.utility import AntelopePf
from mspasspy.ccore.algorithms.deconvolution import CNRDeconEngine
from mspasspy.algorithms.CNRDecon import CNRRFDecon
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.seismic import SeismogramEnsemble,TimeSeries,Seismogram
from mspasspy.ccore.algorithms.amplitudes import MADAmplitude
from mspasspy.algorithms.window import WindowData
from mspasspy.algorithms.basic import ExtractComponent
from mspasspy.util.seismic import number_live
from mspasspy.util.Undertaker import Undertaker
import numpy as np
import dask.bag as dbg
import time


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






def remove_incident(d,ao,nw_end=-3.0,P_component=2):
    if d.dead():
        return d
    stime = max(d.t0,ao.t0)
    etime = min(d.endtime(),ao.endtime())
    x_ao = WindowData(ao,stime,etime)
    s_amp = np.zeros(3)
    n_amp = np.zeros(3)
    snr = np.zeros(3)
    for i in range(3):
        x = ExtractComponent(d,i)
        x_n = WindowData(x,d.t0,nw_end)
        n_amp[i] = MADAmplitude(x_n)
        x_w = WindowData(x,stime,etime)
        amp = np.dot(x_ao.data,x_w.data)
        s_amp[i] = amp
        if n_amp[i]>0.0:
            snr[i] = s_amp[i]/n_amp[i]
        else:
            if s_amp[i]>0.0:
                snr[i]=9999999.9
            else:
                s_amp[i]=1.0
        scaled_ao = TimeSeries(x_ao)
        # Note -amp used so when we add scaled it is subtracting incident
        scaled_ao *= -amp
        # operator += handles irregular start and end time
        # taper may be advised but in this context probably necessary
        x += scaled_ao
        d.data[i,:] = x.data
        
    # post snr metrics before exiting
    snrdoc=dict()
    # not sur mongodb will handle a np array corectly so convert this 
    # to a python list
    snrlist=list()
    namplist=list()
    for i in range(3):
        snrlist.append(snr[i])
        namplist.append(n_amp[i])
    snrdoc['component_snr']=snrlist
    snrdoc['component_noise']=namplist
    # vector sum of incident horizontal snr a more useful metric than 
    # components
    sig_H = 0.0
    noise_H = 0.0
    for i in range(3):
        if i != P_component:
            sig_H += s_amp[i]*s_amp[i]
            noise_H += n_amp[i]*n_amp[i]
    sig_H = np.sqrt(sig_H)
    noise_H = np.sqrt(noise_H)
    if noise_H < np.finfo(float).eps:
        if sig_H < np.finfo(float).eps:
            snr=1.0
        else:
            snr = sig_H/np.finfo(float).eps
    else:
        snr = sig_H/noise_H
    snrdoc['snr_H'] = snr
    d['snr_RF'] = snrdoc
    return d
#from mspasspy.graphics import SeismicPlotter
#import matplotlib.pyplot as plt
def process(sid,db,dbmd,engine,signal_window,noise_window,data_tag=None):
    query = {'source_id' : sid}
    ntotal = db.wf_Seismogram.count_documents(query)
    query['Parrival.median_snr'] = {'$gt' : 2.0}
    query['Parrival.snr_filtered_envelope_peak'] = {'$gt' : 3.0}
    query['Parrival.bandwidth'] = {'$gt' : 8.0}
    if data_tag:
        query['data_tag'] = data_tag
    cursor=db.wf_Seismogram.find(query)
    ens = db.read_data(cursor,collection="wf_Seismogram")
    print('source_id=',sid,' Processing ',len(ens.member),' of ',ntotal)
    decon_ens = SeismogramEnsemble(len(ens.member))
    for d in ens.member:
        if d.live:
            Ptime=d['Ptime']
            d.ator(Ptime)
            decon_output = CNRRFDecon(d,
                        engine,
                        signal_window=signal_window,
                        noise_window=noise_window,
                        bandwidth_subdocument_key="Parrival",
                        return_wavelet=True,
                       )
            rf0 = decon_output[0]
            if rf0.dead():
                # it is a bit inefficient to instantiate an instance of 
                # an Undertaker for each error but CMRRFDecon exceptions 
                # are not common unless the engine is badly configured.
                stedronsky = Undertaker(dbmd)
                stedronsky.bury(rf0)
            else:
                ao = decon_output[1]
                io = decon_output[2]
                rf = remove_incident(rf0, ao)
                # temp for debug with spyder - no save
                #if rf:
                    #plotter=SeismicPlotter(normalize=True,style='wt')
                    #plotter.plot(rf)
                    #plt.show()
                    #continue
                dir="wf_RF"
                s=rf['dfile']
                dfile = "RF"+s
                rf0=dbmd.save_data(rf0,
                              collection='wf_Seismogram',
                              return_data=False,
                              storage_mode='file',
                              dir=dir,
                              dfile=dfile,
                              data_tag='CNRRFDecon_data_raw')
                rf=dbmd.save_data(rf,
                              collection='wf_Seismogram',
                              return_data=True,
                              storage_mode='file',
                              dir=dir,
                              dfile=dfile,
                              data_tag='CNRRFDecon_data')
            
                if rf.live:
                    wfid = rf['_id']
                    ao['parent_wfid'] = wfid
                    io['parent_wfid'] = wfid
                    dfile = "ao_" + s
                    dbmd.save_data(ao,collection='wf_TimeSeries',storage_mode='file',dir=dir,dfile=dfile,data_tag='CNRRFDecon_ao')
                    dfile = "io_" + s
                    dbmd.save_data(io,collection='wf_TimeSeries',
                             storage_mode='file',
                             dir=dir,dfile=dfile,
                             data_tag='CNRRFDecon_io')
                    decon_ens.member.append(rf)
        else:
            print("WARNING:  ensemble member was marked dead - has to be an abortion")
    if number_live(decon_ens)>0:
        decon_ens.set_live()
    return decon_ens
    
pf=AntelopePf('CNRDeconEngine.pf')
engine = CNRDeconEngine(pf)
signal_window=TimeWindow(-10.0,100.0)
noise_window=TimeWindow(-200.0,-5.0)

sidlist=db.wf_Seismogram.distinct('source_id')
t0=time.time()
nprocessed=0
for sid in sidlist:
    print("Working on source_id=",sid)
    rfens = process(sid,db,dbmd,engine,signal_window,noise_window)
    nprocessed+=1
#mydata = dbg.from_sequence(sidlist)
#mydata = mydata.map(process,db,engine,signal_window,noise_window)
#mydata = mydata.map(terminator)
#mydata.compute()
t=time.time()
print("Elapsed time=",t-t0)