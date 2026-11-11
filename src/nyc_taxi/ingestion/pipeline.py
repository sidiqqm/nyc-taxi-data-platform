"""Ingestion of one file (decide, download, validate, publish, record) and of a batch of files."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from nyc_taxi.common.config import Settings, SourceConfig
from nyc_taxi.ingestion.http_client import DownloadError, RemoteInfo, SourceClient
from nyc_taxi.ingestion.manifest import RunLog, read_sidecar, utc_now_iso, write_sidecar
from nyc_taxi.ingestion.months import split_period
from nyc_taxi.ingestion.validators import ValidationError, validate

logger = logging.getLogger(__name__)


class Outcome(StrEnum):
    DOWNLOADED = "downloaded"  # new file
    REPLACED = "replaced"  # source changed (or --force): file overwritten
    SKIPPED = "skipped_unchanged"  # already ingested, source unchanged
    NOT_AVAILABLE = "not_available"  # source does not have the file (yet)
    FAILED = "failed"


@dataclass
class IngestResult:
    dataset: str
    period: str | None
    outcome: Outcome
    detail: str = ""
    size_bytes: int | None = None
    rows: int | None = None
    schema_changed: bool = False
    dry_run: bool = False
    duration_seconds: float = 0.0

    def as_event(self, run_id: str, source_url: str) -> dict:
        return {
            "timestamp": utc_now_iso(),
            "run_id": run_id,
            "dataset": self.dataset,
            "period": self.period,
            "source_url": source_url,
            "outcome": self.outcome.value,
            "detail": self.detail,
            "size_bytes": self.size_bytes,
            "rows": self.rows,
            "schema_changed": self.schema_changed,
            "dry_run": self.dry_run,
            "duration_seconds": round(self.duration_seconds, 3),
        }


def bronze_path(bronze_dir: Path, source: SourceConfig, period: str | None) -> Path:
    """Where a file lives in Bronze. The same relative path will be the object key in GCS.

    monthly:   <root>/yellow/year=2025/month=01/yellow_tripdata_2025-01.parquet
    reference: <root>/reference/taxi_zone_lookup/taxi_zone_lookup.csv

    The path never contains the ingestion date: re-running must hit the same path (idempotency).
    The ingestion time is recorded in the sidecar instead.
    """
    file_name = source.url_for(period).rsplit("/", 1)[-1]
    if source.kind == "monthly":
        year, month = split_period(period)
        return bronze_dir / source.name / f"year={year}" / f"month={month}" / file_name
    return bronze_dir / "reference" / source.name / file_name


def _source_changed(remote: RemoteInfo, meta: dict) -> bool:
    """True if any version signal the source provides differs from what we recorded."""
    signals = [
        ("etag", remote.etag),
        ("last_modified", remote.last_modified),
        ("size_bytes", remote.content_length),
    ]
    provided = [(key, value) for key, value in signals if value is not None]
    if not provided:
        logger.warning("source gave no ETag, Last-Modified or Content-Length: cannot detect change")
    return any(value != meta.get(key) for key, value in provided)


def decide(remote: RemoteInfo, meta: dict | None, file_exists: bool, force: bool) -> Outcome:
    """Pure decision for a file the source has (HTTP 200). Easy to test: no I/O."""
    if meta is None or not file_exists:
        return Outcome.DOWNLOADED
    if force or _source_changed(remote, meta):
        return Outcome.REPLACED
    return Outcome.SKIPPED


def _ingest(
    source: SourceConfig,
    period: str | None,
    *,
    settings: Settings,
    client: SourceClient,
    run_id: str,
    force: bool,
    dry_run: bool,
) -> IngestResult:
    def result(outcome: Outcome, detail: str = "", **extra) -> IngestResult:
        return IngestResult(source.name, period, outcome, detail, dry_run=dry_run, **extra)

    url = source.url_for(period)
    target = bronze_path(settings.bronze_dir, source, period)

    remote = client.head(url)
    if remote.status in settings.http.not_available_statuses:
        return result(Outcome.NOT_AVAILABLE, f"HTTP {remote.status}: file is not published (yet)")
    if remote.status != 200:
        return result(Outcome.FAILED, f"unexpected HTTP status {remote.status} on HEAD")

    meta = read_sidecar(target)
    outcome = decide(remote, meta, target.exists(), force)
    if outcome is Outcome.SKIPPED:
        return result(outcome, "already ingested and source unchanged")
    if dry_run:
        return result(outcome, "dry run: nothing written")

    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_name(target.name + ".part")
    try:
        download = client.download(url, part)
        facts = validate(part, source.file_format, source.required_columns)
        os.replace(part, target)  # atomic: Bronze never shows a half-written file
    finally:
        part.unlink(missing_ok=True)

    write_sidecar(
        target,
        {
            "sidecar_version": 1,
            "dataset": source.name,
            "period": period,
            "source_url": url,
            "bronze_path": target.relative_to(settings.bronze_dir).as_posix(),
            "file_name": target.name,
            "size_bytes": download.size_bytes,
            "sha256": download.sha256,
            "etag": download.etag,
            "last_modified": download.last_modified,
            "rows": facts.rows,
            "columns": facts.columns,
            "schema_fingerprint": facts.schema_fingerprint,
            "ingested_at": utc_now_iso(),
            "ingestion_run_id": run_id,
            "replaced_sha256": meta.get("sha256") if outcome is Outcome.REPLACED and meta else None,
        },
    )

    schema_changed = bool(meta) and meta.get("schema_fingerprint") != facts.schema_fingerprint
    if schema_changed:
        logger.warning("%s %s: schema changed compared with the previous copy", source.name, period)
    return result(
        outcome,
        size_bytes=download.size_bytes,
        rows=facts.rows,
        schema_changed=schema_changed,
    )


def ingest_one(
    source: SourceConfig,
    period: str | None,
    *,
    settings: Settings,
    client: SourceClient,
    run_log: RunLog,
    run_id: str,
    force: bool = False,
    dry_run: bool = False,
) -> IngestResult:
    """Ingest one file. Never raises: every outcome, including failure, becomes a result."""
    started = time.monotonic()
    try:
        outcome = _ingest(
            source,
            period,
            settings=settings,
            client=client,
            run_id=run_id,
            force=force,
            dry_run=dry_run,
        )
    except (DownloadError, ValidationError, OSError) as exc:
        outcome = IngestResult(source.name, period, Outcome.FAILED, str(exc), dry_run=dry_run)
    except Exception as exc:  # a bug must not stop the other months; keep the traceback
        logger.exception("unexpected error while ingesting %s %s", source.name, period)
        outcome = IngestResult(
            source.name,
            period,
            Outcome.FAILED,
            f"unexpected {type(exc).__name__}: {exc}",
            dry_run=dry_run,
        )

    outcome.duration_seconds = time.monotonic() - started
    if not dry_run:  # a dry run must leave no trace in Bronze, not even a log file
        run_log.append(outcome.as_event(run_id, source.url_for(period)))
    level = logging.ERROR if outcome.outcome is Outcome.FAILED else logging.INFO
    logger.log(
        level, "%s %s -> %s %s", source.name, period or "-", outcome.outcome.value, outcome.detail
    )
    return outcome


def default_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def run_ingestion(
    settings: Settings,
    source_name: str,
    periods: list[str | None],
    *,
    run_id: str | None = None,
    force: bool = False,
    dry_run: bool = False,
    client: SourceClient | None = None,
) -> list[IngestResult]:
    """Ingest every period of one source. A failure in one period does not stop the others."""
    source = settings.source(source_name)
    run_id = run_id or default_run_id()
    client = client or SourceClient(settings.http)
    run_log = RunLog(settings.bronze_dir / "_runs", run_id)
    return [
        ingest_one(
            source,
            period,
            settings=settings,
            client=client,
            run_log=run_log,
            run_id=run_id,
            force=force,
            dry_run=dry_run,
        )
        for period in periods
    ]