"""Metadata recording.

Two artifacts, with different jobs:
  * sidecar  (<file>.meta.json): the CURRENT state of one Bronze file. Written last, so it acts
    as the commit marker: a file without a matching sidecar is treated as not ingested.
  * run log  (_runs/<run_id>.jsonl): an append-only audit trail, one line per attempted file.
    One file per run, so it can later be uploaded to object storage (objects cannot be appended).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def sidecar_path(file_path: Path) -> Path:
    return file_path.with_name(file_path.name + ".meta.json")


def read_sidecar(file_path: Path) -> dict | None:
    path = sidecar_path(file_path)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("unreadable sidecar %s (%s); treating the file as not ingested", path, exc)
        return None


def write_sidecar(file_path: Path, record: dict) -> None:
    """Write atomically: a reader sees the old sidecar or the new one, never half of one."""
    final = sidecar_path(file_path)
    temporary = final.with_name(final.name + ".tmp")
    temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, final)


class RunLog:
    def __init__(self, directory: Path, run_id: str) -> None:
        self.path = directory / f"{run_id}.jsonl"

    def append(self, event: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")