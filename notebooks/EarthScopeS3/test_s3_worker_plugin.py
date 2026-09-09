"""Offline tests; no EarthScope login, AWS requests, or worker processes.

Use the real EarthScope SDK session builder and real botocore credentials,
replacing only the credential service with a deterministic cache and clock.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from earthscope_sdk.client.user._service import UserService
from earthscope_sdk.client.user.models import AwsTemporaryCredentials

import s3_worker_plugin as plugin


class CachedCredentialService:
    def __init__(self, initial_ttl=60):
        self.now = datetime(2026, 9, 9, tzinfo=timezone.utc)
        self.expiry = self.now + timedelta(minutes=initial_ttl)
        self.generation = 0
        self.failure = None
        self.closed = False
        self.calls = 0

    def get_aws_credentials(self, *, role):
        assert role == "s3-miniseed"  # Do not change the existing archive role.
        assert not self.closed
        self.calls += 1
        if self.failure:
            raise self.failure
        if self.expiry - self.now <= timedelta(seconds=30):
            self.generation += 1
            self.expiry = self.now + timedelta(minutes=60)
        return AwsTemporaryCredentials(
            aws_access_key_id=f"FAKE_ACCESS_{self.generation}",
            aws_secret_access_key="NOT_A_REAL_SECRET",
            aws_session_token=f"FAKE_TOKEN_{self.generation}",
            expiration=self.expiry,
        )


@pytest.fixture
def harness(monkeypatch):
    # Do not consult the operator's AWS configuration or instance metadata.
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/dev/null")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")

    def make(initial_ttl=60):
        cache = CachedCredentialService(initial_ttl)
        # No SdkContext: its network/credential endpoint is the one mocked seam.
        service = object.__new__(UserService)
        service.get_aws_credentials = cache.get_aws_credentials
        events = []
        sessions = []

        def session_factory():
            session = service.get_boto3_session()
            credentials = session.get_credentials()
            credentials._time_fetcher = lambda: cache.now
            sessions.append(session)
            return session

        def close_sdk():
            cache.closed = True
            events.append("sdk")

        sdk = SimpleNamespace(
            user=SimpleNamespace(get_boto3_session=Mock(side_effect=session_factory)),
            close=Mock(side_effect=close_sdk),
        )
        factory = Mock(return_value=sdk)
        monkeypatch.setattr(plugin, "EarthScopeClient", factory)
        return SimpleNamespace(
            cache=cache,
            sdk=sdk,
            sessions=sessions,
            events=events,
            factory=factory,
            worker=SimpleNamespace(data={}, plugins={}),
        )

    return make


def test_cached_credentials_keep_real_expiry_and_refresh_past_one_hour(harness):
    h = harness()
    worker_plugin = plugin.S3Worker()
    worker_plugin.setup(h.worker)
    try:
        client = worker_plugin.s3_client
        credentials = client._request_signer._credentials
        # No second credential provider or frozen snapshot may replace the SDK's.
        assert credentials is h.sessions[0].get_credentials()
        assert credentials._expiry_time == h.cache.expiry
        assert not h.cache.closed
        start = h.cache.now
        first = credentials.get_frozen_credentials().access_key

        for minute in (41, 55):
            h.cache.now = start + timedelta(minutes=minute)
            assert credentials.get_frozen_credentials().access_key == first
            assert credentials._expiry_time == start + timedelta(minutes=60)

        h.cache.now = start + timedelta(minutes=61)
        assert credentials.get_frozen_credentials().access_key != first
        assert credentials._expiry_time == h.cache.expiry > h.cache.now
        assert h.cache.generation == 1
        h.cache.now = start + timedelta(minutes=122)
        credentials.get_frozen_credentials()
        assert h.cache.generation == 2
        assert credentials._expiry_time == h.cache.expiry > h.cache.now
        assert h.factory.call_count == 1
        assert h.sdk.user.get_boto3_session.call_count == 1
        assert client.meta.config.request_checksum_calculation == "when_required"
        assert client.meta.config.response_checksum_validation == "when_required"
    finally:
        worker_plugin.teardown(h.worker)


def test_cached_credentials_already_near_expiry_are_not_extended(harness):
    h = harness(initial_ttl=8)
    worker_plugin = plugin.S3Worker()
    worker_plugin.setup(h.worker)
    try:
        credentials = worker_plugin.s3_client._request_signer._credentials
        start = h.cache.now
        assert credentials._expiry_time == start + timedelta(minutes=8)
        first = credentials.get_frozen_credentials().access_key
        h.cache.now = start + timedelta(minutes=9)
        assert credentials.get_frozen_credentials().access_key != first
        assert credentials._expiry_time == h.cache.expiry > h.cache.now
    finally:
        worker_plugin.teardown(h.worker)


def test_mandatory_refresh_failure_is_not_hidden(harness):
    h = harness()
    worker_plugin = plugin.S3Worker()
    worker_plugin.setup(h.worker)
    try:
        credentials = worker_plugin.s3_client._request_signer._credentials
        h.cache.now += timedelta(minutes=61)
        error = RuntimeError("simulated credential endpoint failure")
        h.cache.failure = error
        with pytest.raises(RuntimeError) as raised:
            credentials.get_frozen_credentials()
        assert raised.value is error
        h.cache.failure = None
        credentials.get_frozen_credentials()
        assert credentials._expiry_time == h.cache.expiry > h.cache.now
    finally:
        worker_plugin.teardown(h.worker)


@pytest.mark.parametrize("failure_stage", ["session", "client"])
def test_failed_setup_closes_resources(harness, monkeypatch, failure_stage):
    h = harness()
    error = RuntimeError(f"failed {failure_stage}")
    if failure_stage == "session":
        h.sdk.user.get_boto3_session.side_effect = error
    elif failure_stage == "client":
        monkeypatch.setattr(plugin, "fetch_s3_client", Mock(side_effect=error))
    worker_plugin = plugin.S3Worker()
    with pytest.raises(RuntimeError) as raised:
        worker_plugin.setup(h.worker)
    assert raised.value is error
    h.sdk.close.assert_called_once_with()
    assert "s3client" not in h.worker.data
    worker_plugin.teardown(h.worker)
    h.sdk.close.assert_called_once_with()


def test_custom_key_fetch_and_idempotent_teardown(harness, monkeypatch):
    h = harness()
    h.worker.data["unrelated"] = 42
    worker_plugin = plugin.S3Worker(key="archive")
    real_fetch = plugin.fetch_s3_client

    def instrument_client(session):
        client = real_fetch(session)
        real_close = client.close

        def close_client():
            h.events.append("s3")
            real_close()

        client.close = Mock(side_effect=close_client)
        return client

    with monkeypatch.context() as setup_patch:
        setup_patch.setattr(plugin, "fetch_s3_client", instrument_client)
        worker_plugin.setup(h.worker)
    client = worker_plugin.s3_client
    h.worker.plugins["archive"] = worker_plugin
    monkeypatch.setattr(plugin, "get_worker", lambda: h.worker)
    assert plugin.fetch_s3_client(worker_data_key="archive") is client
    worker_plugin.teardown(h.worker)
    worker_plugin.teardown(h.worker)
    assert h.worker.data == {"unrelated": 42}
    assert h.events == ["s3", "sdk"]
    client.close.assert_called_once_with()
    h.sdk.close.assert_called_once_with()


def test_teardown_before_setup_is_safe():
    worker = SimpleNamespace(data={}, plugins={})
    plugin.S3Worker().teardown(worker)
    assert worker.data == {}
