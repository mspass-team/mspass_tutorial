#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prototype worker plugin for using dask on AWS to allow workers to 
not have to instantiate an s3 client on each submit.   Modeled after 
the mongodb worker plugin.

Created on Mon Mar 23 09:26:00 2026

@author: pavlis
"""
import boto3
from botocore.config import Config
from earthscope_sdk import EarthScopeClient
from dask.distributed import WorkerPlugin, get_worker
import threading
import time

def fetch_s3_client(session=None,worker_data_key="s3client"):
    """
    Generic tool to fetch s3 client for Earthscope s3 archive.  
    
    When session is None (default) the funcion assumes it is running in a parallel environment with dask. 
    In that situation it assumes workers have been previously initialized with the WorkerPlugin "S3Worker"
    and it can fetch the client from the worker data area with the key defined by "worker_data_key".
    
    When session is defined it is used to instantiate a client using the stock Earthscope incantation 
    to fetch the access point. 
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
            message = "fetch_s3_client:  dask worker does not have a valued defined for expected key={}\n".format(worker_data_key)
            message = "Make sure you run S3Worker before you submit anything to the dask cluster"
            raise ValueError(message) from e
    else:
        s3_client = session.client(
                    "s3",
                    config=Config(
                        request_checksum_calculation="when_required",
                        response_checksum_validation="when_required",
                    ),
                )


    return s3_client
    

class S3Worker(WorkerPlugin):
    def __init__(self,key="s3client"):
        self.worker_key=key
    def setup(self,worker):
        client = EarthScopeClient()
        creds = client.user.get_aws_credentials()
        session = boto3.Session(
            aws_access_key_id=creds.aws_access_key_id,
            aws_secret_access_key=creds.aws_secret_access_key.get_secret_value(),
            aws_session_token=creds.aws_session_token.get_secret_value(),
        )

        s3_client = session.client(
                    "s3",
                    config=Config(
                        request_checksum_calculation="when_required",
                        response_checksum_validation="when_required",
                    ),
)

        worker.data[self.worker_key] = s3_client
    def teardown(self,worker):
        s3_client = worker.data.get(self.worker_key)
        s3_client.close()

        
class ImmortalS3Client(threading.Thread):
    """
    Use this class on startup on AWS to create a persistent s3 client 
    that is always available on GeoLab.   
    
    The access point Earthscope uses on AWS to access s3 data has a 
    timeout interval.   Work on geolab can mysteriously stop working 
    with session lasting longer than the timeout.  The object this 
    class creates runs a timer thread that creates a new client 
    and distributes it to all workers when the clock approaches the 
    expirationt time.  The object is initialized on construction 
    but you must run the start method (no args) to spawn the 
    thread to create persistent client.   Note it isn't truly
    immortal but will be automatically stopped when you exit 
    the notebook from you create an instance of this class.  
    """

    def __init__(self,dask_client,heartbeat_interval=10.0,restart_interval=3500.0):
        """
        Class constructor.
        
        :param dask_client:  instance of a dask scheduler normally 
          retrieved from a MsPASS client using the get_scheduler method.
        :param heartbeat_interval:   time interval in seconds to check 
          elapsed time.   Default is 10 s which produces minimal overhead
        :param restart_interval:  timeout period before the s3 client is 
          recreated and pushed to workers.   Needs to be less than the actual 
          session timeout to allow the client to be instantiated on all 
          workers.  Default is 3500 s which allows 100 s for completion on 
          all workers.   
        """
        super().__init__()
        self.dask_client = dask_client
        self.hearbeat_interval = heartbeat_interval
        self.restart_interval = restart_interval
        self.daemon = True  # Ensures the thread dies when the main program exits
        self._start_time = None
        self._elapsed = 0
        self._running = False
        # initialize these to null
        self._exit_event = threading.Event()
        self.number_restarts = 0

    def run(self):
        """
        Python version of a virtual method to start the threaded 
        function it is to run.   Note Thread as a "start" method 
        that needs to be called to actually start the thread.
        For that reason the client is actually not created in the 
        constructor but started here.   
        """
        self._running = True
        self._start_time = time.time()
        s3worker = S3Worker()
        self.dask_client.register_plugin(s3worker)
        
        while not self._exit_event.is_set():
            # Update the elapsed time periodically
            self._elapsed = time.time() - self._start_time
            if self._elapsed > self.restart_interval:
                s3worker = S3Worker()
                self.dask_client.register_plugin(s3worker)
                self._start_time = time.time()
                self.number_restarts += 1
            time.sleep(self.hearbeat_interval)
            
    def status(self):
        """
        Print a status message.  
        """
        print("ImmortalS3Client status")
        print("Current run time since last start=", self._elapsed)
        print("Number of restarts since initial creation=",self.number_restarts)
    def stop(self):
        """Stop the background thread."""
        self._exit_event.set()
        self._running = False
