"""One place to configure logging, so every module logs in the same format."""

from __future__ import annotations

import logging
import time


def configure_logging(level: str = "INFO") -> None:
    logging.Formatter.converter = time.gmtime  # timestamps in UTC
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        force=True,
    )