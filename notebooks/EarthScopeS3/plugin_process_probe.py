"""Offline worker-process probe, launched with a timeout by its pytest test."""

from types import SimpleNamespace

from distributed import Client, LocalCluster

import s3_worker_plugin as plugin


class UnpickleableClient:
    def __init__(self, events):
        self.events = events

    def __reduce__(self):
        raise RuntimeError("An S3 client must never be serialized as task data")

    def close(self):
        self.events.append("s3")


class ProbePlugin(plugin.S3Worker):
    def setup(self, worker):
        worker.auth_probe_events = []
        events = worker.auth_probe_events
        session = SimpleNamespace(client=lambda *a, **kw: UnpickleableClient(events))
        sdk = SimpleNamespace(
            user=SimpleNamespace(get_boto3_session=lambda: session),
            close=lambda: events.append("sdk"),
        )
        original = plugin.EarthScopeClient
        plugin.EarthScopeClient = lambda: sdk
        try:
            super().setup(worker)
        finally:
            plugin.EarthScopeClient = original


def check_client():
    from distributed import get_worker
    from s3_worker_plugin import fetch_s3_client

    worker = get_worker()
    client = fetch_s3_client(worker_data_key="archive")
    return (
        client is worker.plugins["archive"].s3_client and "archive" not in worker.data
    )


def close_events(dask_worker=None):
    return dask_worker.auth_probe_events


if __name__ == "__main__":
    with LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=True,
        memory_limit=0,
        dashboard_address=None,
    ) as cluster:
        with Client(cluster) as client:
            client.register_plugin(ProbePlugin(key="archive"))
            assert client.submit(check_client, pure=False).result(timeout=10)
            client.unregister_worker_plugin("archive")
            assert list(client.run(close_events).values()) == [["s3", "sdk"]]
    print("worker-process registration, fetch, and teardown: PASS")
