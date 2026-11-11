import pytest

from nyc_taxi.common.retry import retry_call
from nyc_taxi.ingestion.http_client import RemoteInfo
from nyc_taxi.ingestion.months import month_range, split_period
from nyc_taxi.ingestion.pipeline import Outcome, decide

REMOTE = RemoteInfo(status=200, etag='"v1"', last_modified="Mon", content_length=10)
META = {"etag": '"v1"', "last_modified": "Mon", "size_bytes": 10}


def test_month_range_crosses_year_boundary():
    assert month_range("2024-11", "2025-02") == ["2024-11", "2024-12", "2025-01", "2025-02"]


def test_month_range_single_month():
    assert month_range("2025-01", "2025-01") == ["2025-01"]


@pytest.mark.parametrize("bad", ["2025-1", "2025-13", "25-01", "2025/01", "jan"])
def test_invalid_period_is_rejected(bad):
    with pytest.raises(ValueError):
        split_period(bad)


def test_start_after_end_is_rejected():
    with pytest.raises(ValueError):
        month_range("2025-02", "2025-01")


@pytest.mark.parametrize(
    ("meta", "file_exists", "force", "remote", "expected"),
    [
        (None, False, False, REMOTE, Outcome.DOWNLOADED),  # never ingested
        (META, False, False, REMOTE, Outcome.DOWNLOADED),  # sidecar exists, file was deleted
        (META, True, False, REMOTE, Outcome.SKIPPED),  # nothing changed
        (META, True, True, REMOTE, Outcome.REPLACED),  # forced
        (META, True, False, RemoteInfo(200, '"v2"', "Mon", 10), Outcome.REPLACED),  # etag
        (META, True, False, RemoteInfo(200, '"v1"', "Tue", 10), Outcome.REPLACED),  # last-modified
        (META, True, False, RemoteInfo(200, '"v1"', "Mon", 99), Outcome.REPLACED),  # size
        (META, True, False, RemoteInfo(200, None, None, None), Outcome.SKIPPED),  # no signals
    ],
)
def test_decide(meta, file_exists, force, remote, expected):
    assert decide(remote, meta, file_exists, force) is expected


def test_retry_waits_with_growing_delays_then_succeeds():
    calls, sleeps = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("boom")
        return "ok"

    result = retry_call(
        flaky, retry_on=(ConnectionError,), max_attempts=4, base_delay=2.0, sleep=sleeps.append
    )
    assert result == "ok"
    assert len(sleeps) == 2
    assert 1.0 <= sleeps[0] <= 2.0  # base 2 * 2**0, jitter 0.5-1.0
    assert 2.0 <= sleeps[1] <= 4.0  # base 2 * 2**1


def test_retry_gives_up_and_reraises():
    with pytest.raises(ConnectionError):
        retry_call(
            lambda: (_ for _ in ()).throw(ConnectionError("down")),
            retry_on=(ConnectionError,),
            max_attempts=2,
            base_delay=0.0,
            sleep=lambda s: None,
        )


def test_retry_does_not_catch_other_exceptions():
    calls = []

    def broken():
        calls.append(1)
        raise ValueError("bug")

    with pytest.raises(ValueError):
        retry_call(broken, retry_on=(ConnectionError,), max_attempts=3, base_delay=0.0)
    assert len(calls) == 1