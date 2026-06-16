#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prototype worker plugin for using dask on GeoLab to allow workers to 
not have to instantiate an s3 client on each submit.   Modeled after 
the mongodb worker plugin.

This is kind of version 2 of this module.  The early earthscope client 
was not asynchronous.  This uses the AsyncEarthScopeClient with modifications.
Note because of this use all uses of ththe s3 client it handles 
must use async and await clauses as appropriate.   


Created on Mon Mar 23 09:26:00 2026

@author: pavlis
"""

from botocore.config import Config
import earthscope_sdk as ES
from dask.distributed import WorkerPlugin, get_worker
import asyncio


async def fetch_s3_client(session=None,worker_data_key="s3client"):
    """
    Generic tool to fetch s3 client for Earthscope s3 archive.   
    
    When session is None (default) the function assumes it is running in a parallel environment with dask. 
    In that situation it assumes workers have been previously initialized with the WorkerPlugin "S3Worker"
    and it can fetch the client from the worker data area with the key defined by "worker_data_key".
    
    When session is defined it is used to instantiate a client from that session.  
    That mode would be the norm for a serial workflow.  That session must have been created 
    from the AsyncEarthScopeClient and instantiated with the client's
    `get_aioboto3_session` method - needed to get valid credentials on GeoLab.  

    """
    if session is None:
        try:
            worker = get_worker()
        except Exception as e:
            raise ValueError(
                    "fetch_s3_client: this function must be "
                    "executed within a Dask worker context so that get_worker() succeeds and an "
                    "s3 client is available via a worker plugin."
                ) from e
        try:
            s3_client = worker.data[worker_data_key]

        except KeyError as e:
            message = "fetch_s3_client:  dask worker does not have a value defined for expected key={}\n".format(worker_data_key)
            message = "Make sure you create an S3Worker instance and then distribute it to the cluster before you submit anything to the dask cluster"
            raise ValueError(message) from e
    else:
        s3_client = await session.client(
                    "s3",
                    config=Config(
                    response_checksum_validation="when_required",
                ),
            ).__aenter__()
    return s3_client
    

class AsyncS3Worker(WorkerPlugin):
    def __init__(self,key="s3client"):
        self.worker_key=key
    async def setup(self,worker):
        print("entered setup method")
        client = ES.AsyncEarthScopeClient()
        print("trying to create session object")
        session = await client.user.get_aioboto3_session()
        s3_client = await session.client(
                "s3",
                config=Config(
                # checksum verification doesn't work with S3 object lambda
                response_checksum_validation="when_required",
                ),
            ).__aenter__()

        worker.data[self.worker_key] = s3_client
    def teardown(self,worker):
        s3_client = worker.data.get(self.worker_key)
        asyncio.run(s3_client.__aexit__(None, None, None))
        

