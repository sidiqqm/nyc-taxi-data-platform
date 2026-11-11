import hashlib
import json

import pytest
from conftest import FILE_PATH, PERIOD, Served, parquet_bytes

from nyc_taxi.ingestion.cli import main as cli_main
from nyc_taxi.ingestion.pipeline import Outcome, bronze_path, run_ingestion


def ingest(settings, **kwargs):
    return run_ingestion(settings, "yellow", [PERIOD], **kwargs)[0]


def target(settings):
    return bronze_path(settings.bronze_dir, settings.source("yellow"), PERIOD)


def sidecar(settings):
    path = target(settings)
    return json.loads(path.with_name(path.name + ".meta.json").read_text())


def test_downloads_validates_and_records_metadata(server, settings):
    body = parquet_bytes(rows=100)
    server.files[FILE_PATH] = Served(body)

    result = ingest(settings)

    assert result.outcome is Outcome.DOWNLOADED
    assert (
        target(settings)
        .as_posix()
        .endswith("bronze/yellow/year=2025/month=01/yellow_tripdata_2025-01.parquet")
    )
    assert target(settings).read_bytes() == body
    meta = sidecar(settings)
    assert meta["sha256"] == hashlib.sha256(body).hexdigest()
    assert meta["rows"] == 100
    assert meta["etag"] == '"v1"'
    assert meta["bronze_path"] == "yellow/year=2025/month=01/yellow_tripdata_2025-01.parquet"
    assert not list(target(settings).parent.glob("*.part"))


def test_second_run_changes_nothing(server, settings):
    server.files[FILE_PATH] = Served(parquet_bytes())
    ingest(settings)
    before_meta = sidecar(settings)
    before_mtime = target(settings).stat().st_mtime_ns
    gets_before = server.get_counts[FILE_PATH]

    result = ingest(settings)

    assert result.outcome is Outcome.SKIPPED
    assert sidecar(settings) == before_meta  # same ingested_at: nothing was rewritten
    assert target(settings).stat().st_mtime_ns == before_mtime
    assert server.get_counts[FILE_PATH] == gets_before  # no second download


def test_republished_file_is_replaced_and_schema_change_is_flagged(server, settings):
    server.files[FILE_PATH] = Served(parquet_bytes(rows=100))
    ingest(settings)
    first_sha = sidecar(settings)["sha256"]

    new_body = parquet_bytes(rows=120, extra_column=True)
    server.files[FILE_PATH] = Served(new_body, etag='"v2"')
    result = ingest(settings)

    assert result.outcome is Outcome.REPLACED
    assert result.schema_changed is True
    meta = sidecar(settings)
    assert meta["rows"] == 120
    assert meta["replaced_sha256"] == first_sha
    assert target(settings).read_bytes() == new_body


@pytest.mark.parametrize("status", [403, 404])
def test_unpublished_file_is_not_available_not_a_failure(server, settings, status):
    server.files[FILE_PATH] = Served(b"", status=status)
    result = ingest(settings)
    assert result.outcome is Outcome.NOT_AVAILABLE
    assert not target(settings).exists()


def test_transient_server_errors_are_retried(server, settings):
    server.files[FILE_PATH] = Served(parquet_bytes(), fail_first_gets=2)
    result = ingest(settings)
    assert result.outcome is Outcome.DOWNLOADED
    assert server.get_counts[FILE_PATH] == 3  # two 503s, then success


def test_persistent_server_error_fails_after_max_attempts(server, settings):
    server.files[FILE_PATH] = Served(parquet_bytes(), fail_first_gets=99)
    result = ingest(settings)
    assert result.outcome is Outcome.FAILED
    assert server.get_counts[FILE_PATH] == 3
    assert not target(settings).exists()


def test_truncated_download_fails_and_keeps_previous_good_copy(server, settings):
    good = parquet_bytes(rows=100)
    server.files[FILE_PATH] = Served(good)
    ingest(settings)
    good_meta = sidecar(settings)

    server.files[FILE_PATH] = Served(parquet_bytes(rows=200), etag='"v2"', truncate_to=200)
    result = ingest(settings)

    assert result.outcome is Outcome.FAILED
    assert target(settings).read_bytes() == good  # old file untouched
    assert sidecar(settings) == good_meta
    assert not list(target(settings).parent.glob("*.part"))


def test_corrupt_file_fails_validation_and_keeps_previous_good_copy(server, settings):
    good = parquet_bytes()
    server.files[FILE_PATH] = Served(good)
    ingest(settings)

    server.files[FILE_PATH] = Served(b"x" * 500, etag='"v2"')
    result = ingest(settings)

    assert result.outcome is Outcome.FAILED
    assert target(settings).read_bytes() == good
    assert sidecar(settings)["etag"] == '"v1"'


def test_force_downloads_again(server, settings):
    server.files[FILE_PATH] = Served(parquet_bytes())
    ingest(settings)
    result = ingest(settings, force=True)
    assert result.outcome is Outcome.REPLACED
    assert server.get_counts[FILE_PATH] == 2


def test_dry_run_writes_nothing(server, settings):
    server.files[FILE_PATH] = Served(parquet_bytes())
    result = ingest(settings, dry_run=True)
    assert result.outcome is Outcome.DOWNLOADED
    assert result.dry_run is True
    assert not settings.bronze_dir.exists()  # no file, no sidecar, no run log, no folders
    assert server.get_counts[FILE_PATH] == 0


def test_one_failing_month_does_not_stop_the_others(server, settings):
    server.files["/yellow_tripdata_2025-01.parquet"] = Served(b"junk" * 10)
    server.files["/yellow_tripdata_2025-02.parquet"] = Served(parquet_bytes())
    results = run_ingestion(settings, "yellow", ["2025-01", "2025-02", "2025-03"])
    outcomes = [r.outcome for r in results]
    assert outcomes == [Outcome.FAILED, Outcome.DOWNLOADED, Outcome.NOT_AVAILABLE]


def test_reference_csv_is_ingested_and_validated(server, settings):
    server.files["/taxi_zone_lookup.csv"] = Served(b"LocationID,Borough,Zone\n1,EWR,Newark\n")
    result = run_ingestion(settings, "zones", [None])[0]
    assert result.outcome is Outcome.DOWNLOADED
    path = settings.bronze_dir / "reference" / "zones" / "taxi_zone_lookup.csv"
    assert path.exists()

    server.files["/taxi_zone_lookup.csv"] = Served(b"LocationID,Zone\n1,Newark\n", etag='"v2"')
    assert run_ingestion(settings, "zones", [None])[0].outcome is Outcome.FAILED


def test_run_log_has_one_event_per_attempt(server, settings):
    server.files[FILE_PATH] = Served(parquet_bytes())
    ingest(settings, run_id="run-a")
    ingest(settings, run_id="run-b")
    lines = (settings.bronze_dir / "_runs" / "run-a.jsonl").read_text().splitlines()
    event = json.loads(lines[0])
    assert (event["outcome"], event["dataset"], event["period"]) == ("downloaded", "yellow", PERIOD)
    second = json.loads((settings.bronze_dir / "_runs" / "run-b.jsonl").read_text().splitlines()[0])
    assert second["outcome"] == "skipped_unchanged"


def test_cli_exit_codes(server, settings, tmp_path):
    server.files[FILE_PATH] = Served(parquet_bytes())
    config = tmp_path / "cfg.toml"
    config.write_text(
        f'[paths]\nbronze_dir = "{(tmp_path / "cli_bronze").as_posix()}"\n'
        "[http]\nmax_attempts = 2\nbackoff_base_seconds = 0.0\n"
        '[sources.yellow]\nkind = "monthly"\nfile_format = "parquet"\n'
        f'url_template = "{server.base_url}/yellow_tripdata_{{period}}.parquet"\n'
    )
    base = ["yellow", "--config", str(config)]

    assert cli_main([*base, "--start", PERIOD]) == 0
    assert cli_main([*base, "--start", "2025-01", "--end", "2025-02"]) == 0  # 02 not available
    assert cli_main([*base, "--start", "2025-01", "--end", "2025-02", "--strict"]) == 1
    assert cli_main([*base]) == 2  # --start missing
    assert cli_main(["nope", "--config", str(config)]) == 2  # unknown source