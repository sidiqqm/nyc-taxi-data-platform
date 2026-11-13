from __future__ import annotations

import argparse
import dataclasses
import logging
import sys
from pathlib import Path

from nyc_taxi.common.config import DEFAULT_CONFIG_PATH, ConfigError, load_settings
from nyc_taxi.common.logging_setup import configure_logging
from nyc_taxi.ingestion.months import month_range
from nyc_taxi.ingestion.pipeline import (
    IngestResult,
    Outcome,
    run_ingestion,
    run_upload_only,
)

logger = logging.getLogger(__name__)

EXIT_OK, EXIT_FAILED, EXIT_USAGE = 0, 1, 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m nyc_taxi.ingestion",
        description="Download source files into the Bronze layer (incremental and idempotent).",
    )
    parser.add_argument("source", help="source name from the config file, e.g. yellow")
    parser.add_argument("--start", help="first month YYYY-MM (monthly sources)")
    parser.add_argument("--end", help="last month YYYY-MM (default: same as --start)")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--bronze-dir", type=Path, help="override paths.bronze_dir from the config")
    parser.add_argument("--run-id", help="label for this run (default: UTC timestamp)")
    parser.add_argument("--force", action="store_true", help="download again even if unchanged")
    parser.add_argument("--dry-run", action="store_true", help="check the source, write nothing")
    parser.add_argument(
        "--strict", action="store_true", help="exit non-zero if any month is not available"
    )
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--upload", action="store_true", help="upload ingested files to GCS after ingestion"
    )
    parser.add_argument(
        "--upload-only",
        action="store_true",
        help="skip download; upload already ingested Bronze files to GCS",
    )
    parser.add_argument(
        "--upload-force",
        action="store_true",
        help="re-upload even when the remote copy is up to date",
    )
    return parser


def _make_uploader(args: argparse.Namespace, settings):
    from nyc_taxi.ingestion.gcs import GcsUploader, UploadError

    if settings.gcs is None:
        raise ConfigError("--upload requires a [gcs] section in the config file")
    try:
        return GcsUploader(settings.gcs.bucket)
    except UploadError as exc:
        raise ConfigError(str(exc)) from exc


def _print_results(results: list[IngestResult]) -> None:
    print(f"\n{'period':<9} {'outcome':<20} {'rows':>12} {'size_mb':>9} {'gcs':<4}  detail")
    for r in results:
        size = f"{r.size_bytes / 1_000_000:.1f}" if r.size_bytes is not None else "-"
        rows = f"{r.rows:,}" if r.rows is not None else "-"
        gcs = "yes" if r.uploaded else "-"
        print(
            f"{r.period or '-':<9} {r.outcome.value:<20} {rows:>12} {size:>9} {gcs:<4}  {r.detail}"
        )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)

    if args.upload and args.upload_only:
        print("error: --upload and --upload-only are mutually exclusive", file=sys.stderr)
        return EXIT_USAGE

    try:
        settings = load_settings(args.config)
        if args.bronze_dir:
            settings = dataclasses.replace(settings, bronze_dir=args.bronze_dir)
        source = settings.source(args.source)

        if source.kind == "monthly":
            if not args.start:
                raise ValueError(f"source {source.name!r} is monthly: --start is required")
            periods: list[str | None] = list(month_range(args.start, args.end or args.start))
        else:
            periods = [None]

        uploader = _make_uploader(args, settings) if (args.upload or args.upload_only) else None
    except (ConfigError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE

    if args.upload_only:
        assert uploader is not None
        results = run_upload_only(
            settings,
            args.source,
            periods,
            uploader=uploader,
            upload_force=args.upload_force,
        )
    else:
        results = run_ingestion(
            settings,
            args.source,
            periods,
            run_id=args.run_id,
            force=args.force,
            dry_run=args.dry_run,
            uploader=uploader,
            upload_force=args.upload_force,
        )

    _print_results(results)

    failed = [r for r in results if r.outcome is Outcome.FAILED]
    missing = [r for r in results if r.outcome is Outcome.NOT_AVAILABLE]
    if len(results) > 1 and len(missing) == len(results):
        logger.warning(
            "every requested period is not available: check network access and "
            "not_available_statuses before assuming the files are unpublished"
        )
    if failed or (args.strict and missing):
        return EXIT_FAILED
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())