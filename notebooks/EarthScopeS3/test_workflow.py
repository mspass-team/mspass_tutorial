"""Native waveform tests with fake network/database boundaries, not live S3."""

import io
import pickle
import weakref
from contextlib import contextmanager
from types import SimpleNamespace

import numpy as np
import obspy
import pandas as pd
import pytest
from botocore.exceptions import ClientError
from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from mspasspy.ccore.utility import ErrorSeverity

import s3_day_workflow as workflow
import s3segment_reader as reader


def aws_error(code, operation="HeadObject"):
    return ClientError(
        {
            "Error": {"Code": code, "Message": "test failure"},
            "ResponseMetadata": {"HTTPStatusCode": 403, "RequestId": "TEST_REQUEST"},
        },
        operation,
    )


class FakeS3:
    def __init__(self, payload=b"", missing=(), error=None):
        self.payload, self.missing, self.error = payload, missing, error
        self.heads = []
        self.body = None

    def head_object(self, *, Bucket, Key):
        self.heads.append(Key)
        if self.error:
            raise self.error
        if Key in self.missing:
            raise aws_error("404")

    def get_object(self, **kwargs):
        self.body = io.BytesIO(self.payload)
        return {"Body": self.body}


def miniseed(rates=(10,)):
    stream = obspy.Stream()
    for index, rate in enumerate(rates):
        stream += obspy.Trace(
            np.arange(100, dtype=np.int32),
            header={
                "network": "TA",
                "station": "ABC",
                "channel": "BHZ",
                "location": f"{index:02d}",
                "sampling_rate": rate,
                "starttime": obspy.UTCDateTime(2014, 1, 1),
            },
        )
    buffer = io.BytesIO()
    stream.write(buffer, format="MSEED")
    return buffer.getvalue()


def request_document():
    start = obspy.UTCDateTime(2014, 1, 1).timestamp
    return {
        "s3objects": ["day"],
        "channel_select": "B*",
        "start": [start + 1],
        "end": [start + 2],
        "arrival": [{"time": start + 1, "arid": 7}],
        "jday_tag": "2014_1",
    }


@pytest.mark.parametrize("code", ["403", "AccessDenied", "ExpiredToken", "SlowDown"])
def test_authentication_and_service_errors_do_not_try_other_versions(code, monkeypatch):
    error = aws_error(code)
    client = FakeS3(error=error)
    monkeypatch.setattr(reader, "fetch_s3_client", lambda: client)
    with pytest.raises(ClientError) as raised:
        reader.get_s3_segments(request_document())
    assert raised.value is error
    assert client.heads == ["day"]


def test_refresh_runtime_error_is_not_converted_to_dead_data(monkeypatch):
    error = RuntimeError("credential refresh failed")
    client = FakeS3(error=error)
    monkeypatch.setattr(reader, "fetch_s3_client", lambda: client)
    with pytest.raises(RuntimeError) as raised:
        reader.get_s3_segments(request_document())
    assert raised.value is error


def test_native_memory_error_stops_processing(monkeypatch):
    error = MemoryError("test allocation failure")
    client = FakeS3(miniseed())
    monkeypatch.setattr(reader, "fetch_s3_client", lambda: client)

    def fail(stream):
        raise error

    monkeypatch.setattr(reader, "Stream2TimeSeriesEnsemble", fail)
    with pytest.raises(MemoryError) as raised:
        reader.get_s3_segments(request_document())
    assert raised.value is error


@pytest.mark.parametrize("fails", [False, True])
def test_serial_reader_closes_owned_client(fails, monkeypatch):
    error = aws_error("403") if fails else None
    client = FakeS3(miniseed(), error=error)
    closed = []
    client.close = lambda: closed.append(True)
    session = object()

    def fetch(actual_session):
        assert actual_session is session
        return client

    monkeypatch.setattr(reader, "fetch_s3_client", fetch)
    if fails:
        with pytest.raises(ClientError) as raised:
            reader.get_s3_segments(request_document(), session=session)
        assert raised.value is error
    else:
        assert reader.get_s3_segments(request_document(), session=session).live
    assert closed == [True]


def test_missing_base_falls_back_and_closes_body():
    client = FakeS3(miniseed(), missing={"day", "day#3"})
    stream, errors = reader.get_object_with_iris_versions(client, "day", maxversion=3)
    assert client.heads == ["day", "day#3", "day#2"]
    assert len(stream) == 1
    assert client.body.closed


def test_all_versions_missing_and_invalid_miniseed_are_data_failures():
    client = FakeS3(missing={"day", "day#1"})
    stream, errors = reader.get_object_with_iris_versions(client, "day", maxversion=1)
    assert stream is None
    assert errors.size() > 0
    invalid = FakeS3(b"not miniSEED")
    stream, errors = reader.get_object_with_iris_versions(invalid, "day")
    assert stream is None
    assert invalid.body.closed


def test_body_read_error_propagates_and_closes():
    error = OSError("truncated S3 response")

    class FailedBody(io.BytesIO):
        def read(self, *args):
            raise error

    body = FailedBody()
    client = FakeS3()
    client.get_object = lambda **kwargs: {"Body": body}
    with pytest.raises(OSError) as raised:
        reader.get_object_with_iris_versions(client, "day")
    assert raised.value is error
    assert body.closed


def test_get_error_propagates_without_decode_or_version_fallback():
    client = FakeS3()
    error = aws_error("ExpiredToken", "GetObject")

    def fail(**kwargs):
        raise error

    client.get_object = fail
    with pytest.raises(ClientError) as raised:
        reader.get_object_with_iris_versions(client, "day")
    assert raised.value is error
    assert client.heads == ["day"]


@pytest.mark.parametrize("rates", [(10,), (10, 20)])
def test_real_miniseed_decode_detrend_and_window(rates, monkeypatch):
    client = FakeS3(miniseed(rates))
    monkeypatch.setattr(reader, "fetch_s3_client", lambda: client)
    converted = []
    converter = reader.Stream2TimeSeriesEnsemble

    def convert(stream):
        converted.append(len(stream))
        return converter(stream)

    monkeypatch.setattr(reader, "Stream2TimeSeriesEnsemble", convert)
    result = reader.get_s3_segments(request_document(), detrend_type="simple")
    assert result.live, str(result.elog)
    assert converted == [len(rates)]
    assert len(result.member) == len(rates)
    for member in result.member:
        assert member["arid"] == 7
        np.testing.assert_allclose(np.asarray(member.data), 0, atol=1e-10)


def test_paginated_index_deduplicates_versioned_names():
    def pages(**kwargs):
        assert kwargs["Prefix"] == "miniseed/TA/2014/001/"
        yield {"Contents": [{"Key": f"object{i}"} for i in range(1000)]}
        yield {"Contents": [{"Key": "object1000"}, {"Key": "object0#2"}]}

    client = SimpleNamespace(get_paginator=lambda name: SimpleNamespace(paginate=pages))
    names, metadata = reader.fetch_net_day_list(client, "TA", 2014, 1)
    assert len(names) == 1001
    assert names == sorted(set(names))
    assert metadata == {"server_data": {}}


def test_index_error_propagates():
    error = aws_error("ExpiredToken", "ListObjectsV2")

    def pages(**kwargs):
        raise error

    client = SimpleNamespace(get_paginator=lambda name: SimpleNamespace(paginate=pages))
    with pytest.raises(ClientError) as raised:
        reader.fetch_net_day_list(client, "TA", 2014, 1)
    assert raised.value is error


def test_day_group_uses_union_without_multiplying_arrivals(monkeypatch):
    beginning = obspy.UTCDateTime(2014, 12, 31).timestamp
    frame = pd.DataFrame(
        [
            {"time": beginning + 100, "net": "TA", "sta": "ABC", "arid": 1},
            {"time": beginning + 86390, "net": "TA", "sta": "ABC", "arid": 2},
        ]
    )
    queries = []

    def find(query):
        queries.append(query)
        return [
            {"year": yr, "jday": day, "net": "TA", "sta": "ABC", "s3key": name}
            for yr, day, name in [(2014, 365, "a"), (2015, 1, "b")]
        ]

    db = {"wf_s3": SimpleNamespace(find=find)}
    monkeypatch.setattr(reader, "fetch_dbhandle", lambda handle: db)
    days, errors = reader.build_day_processing_blocks(
        frame, -10, 20, db, pad=0, auxkeys=["arid"]
    )
    assert errors == []
    assert len(days) == 1
    assert queries == [
        {"$or": [{"year": 2014, "jday": 365}, {"year": 2015, "jday": 1}]}
    ]
    doc = days[0][1][0]
    assert doc["s3objects"] == ["a", "b"]
    assert [a["arid"] for a in doc["arrival"]] == [1, 2]
    assert len(doc["start"]) == len(doc["end"]) == 2


def make_ensemble(value=1, live=True):
    ens = TimeSeriesEnsemble()
    if live:
        trace = TimeSeries(8)
        trace.set_live()
        trace["arid"] = value
        trace.data[0] = value
        ens.member.append(trace)
        ens.set_live()
    else:
        ens.elog.log_error("read", "missing test data", ErrorSeverity.Invalid)
    ens["jday_tag"] = "2014_1"
    return ens


def test_completion_releases_sources_before_pickle_and_preserves_output(monkeypatch):
    inputs = [make_ensemble(1), make_ensemble(2)]
    references = [weakref.ref(ens) for ens in inputs]
    saved = {}

    @contextmanager
    def open_file(key, mode):
        assert mode == "wb"
        assert inputs == [None, None]
        assert all(reference() is None for reference in references)
        buffer = io.BytesIO()
        yield buffer
        saved[key] = buffer.getvalue()

    monkeypatch.setattr(workflow, "fetch_dbhandle", lambda handle: {})
    assert (
        workflow.save_jday_outputs(
            inputs,
            None,
            SimpleNamespace(open=open_file),
            "s3://scratch/user-prefix/",
            2014,
            verbose=True,
        )
        is True
    )
    assert list(saved) == ["s3://scratch/user-prefix/2014/2014_1.pickle"]
    result = pickle.loads(next(iter(saved.values())))
    assert isinstance(result, TimeSeriesEnsemble)
    assert result.live and result["jday_tag"] == "2014_1"
    assert [t["arid"] for t in result.member] == [1, 2]
    assert [t.data[0] for t in result.member] == [1, 2]


@pytest.mark.parametrize("failure_stage", ["open", "dump", "close"])
def test_write_errors_escape_completion(failure_stage, monkeypatch):
    error = OSError(f"test {failure_stage} error")

    @contextmanager
    def open_file(key, mode):
        if failure_stage == "open":
            raise error
        yield io.BytesIO()
        if failure_stage == "close":
            raise error

    def fail_dump(*args):
        raise error

    if failure_stage == "dump":
        monkeypatch.setattr(workflow.pickle, "dump", fail_dump)
    monkeypatch.setattr(workflow, "fetch_dbhandle", lambda handle: {})
    with pytest.raises(OSError) as raised:
        workflow.save_jday_outputs(
            [make_ensemble()],
            None,
            SimpleNamespace(open=open_file),
            "s3://scratch/u",
            2014,
        )
    assert raised.value is error


def test_dead_and_empty_days_record_failures_without_writing(monkeypatch):
    documents = []
    db = {"s3_read_failures": SimpleNamespace(insert_many=documents.extend)}
    monkeypatch.setattr(workflow, "fetch_dbhandle", lambda handle: db)
    assert not workflow.save_jday_outputs(
        [make_ensemble(live=False)], db, None, "scratch/u", 2014
    )
    assert len(documents) == 1
    assert documents[0]["elog_content"]
    assert not workflow.save_jday_outputs([], db, None, "scratch/u", 2014)
    assert workflow.count_saved(None, True) == 1
    assert workflow.count_saved(1, False) == 1
