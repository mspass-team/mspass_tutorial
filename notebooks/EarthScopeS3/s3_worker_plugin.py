#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prototype worker plugin for using dask on AWS to allow workers to
not have to instantiate an s3 client on each submit.   Modeled after
the mongodb worker plugin.

Specific to the supplied EarthScope/GeoLab workflow. Live deployment validation
is still required; the tests exercise credential lifecycle without AWS access.

Created on Mon Mar 23 09:26:00 2026

@author: pavlis
"""

from contextlib import ExitStack
from botocore.config import Config
from earthscope_sdk import EarthScopeClient
from dask.distributed import WorkerPlugin, get_worker


def fetch_s3_client(session=None, worker_data_key="s3client"):
    """
    Generic tool to fetch s3 client for Earthscope s3 archive.

    When session is None (default) the funcion assumes it is running in a parallel environment with dask.
    In that situation it assumes workers have been previously initialized with the WorkerPlugin "S3Worker"
    and it fetches the client from worker.plugins using "worker_data_key" as the plugin name.

    When session is defined a client is instantiated with the client method of the session object.
    Note on GeoLab the session should be constructed using this construct:
    ```
        client = EarthScopeClient()
        session = client.user.get_boto3_session()
    ```
    There are other ways to do that, but Earthscope advises that construct to avoid a problem
    timeout of credentials after an hour that can happen otherwise.
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
            s3_client = worker.plugins[worker_data_key].s3_client

        except KeyError as e:
            message = "fetch_s3_client:  dask worker has no S3 plugin registered with name={}\n".format(
                worker_data_key
            )
            message += "Register S3Worker before submitting tasks to the dask cluster"
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
    """
    Dask worker plugin to create a worker resident client on each dask worker.

    An S3 client requires significant time to construct for a variety of reasons.
    It is known to be a really serious bottleneck if s3 clients are instantiated
    inside a parallel workflow to access s3.   This class can be used to
    create what is called a worker plugin for dask.   The standard use in a
    python script is:
    ```
        dask_client = mspass_client.get_scheduler()
        s3plugin = S3Worker()
        dask_client.register_plugin(s3plugin)
    ```
    That creates and loads a memory resident s3 client in each worker's memory space.
    Processing functions that need access to s3 should then use the function
    `fetch_s3_client` to get a reference to the memory resident s3 client.
    That produces near zero overhead in a worker process particularly compared to
    the time needed to instantiate a new instance of an s3 client.
    """

    def __init__(self, key="s3client"):
        """
        Standard constructor.

        Normal use requires no arguments.  One could change the key argument
        but that is not advised.  The S3 client is held by a named worker plugin, outside the spillable
        worker.data dictionary.  The key selects that plugin name.
        The only reason to change it would be if some other worker plugin
        used the same key, which should never happen.
        """
        self.worker_key = key
        self.name = key

    def setup(self, worker):
        """
        Required method for a worker plugin.  This acts like a secondary constructor.
        It is involked on each worker when this object is pushed to
        each worker with the register_plugin method of the dask client.
        """
        # Keep the SDK's refresh provider and real expiration intact.  A frozen
        # credential snapshot has no expiration, and a new SDK session may reuse
        # cached credentials; inventing a new expiry would extend their lifetime
        # locally without extending their actual AWS validity.
        with ExitStack() as cleanup:
            esclient = EarthScopeClient()
            cleanup.callback(esclient.close)
            session = esclient.user.get_boto3_session()
            s3_client = fetch_s3_client(session)
            cleanup.callback(s3_client.close)
            # A client is a worker resource, not task data: keeping it in
            # worker.data could ask Dask to pickle/spill its credentials.
            self.s3_client = s3_client
            self._cleanup = cleanup.pop_all()

    def teardown(self, worker):
        """
        Required method for a worker plugin.  This method is effectively a destructor
        called when the class goes out of scope.   It is essential in this case to
        avoid a resource leak as it properly closes the clients connections to s3.
        """
        cleanup = getattr(self, "_cleanup", None)
        if cleanup is not None:
            self._cleanup = None
            self.s3_client = None
            cleanup.close()
