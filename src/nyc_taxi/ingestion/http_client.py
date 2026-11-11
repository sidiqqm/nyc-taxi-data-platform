from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import requests

from nyc_taxi.common.config import HttpSettings
from nyc_taxi.common.retry import retry_call

logger = logging.getLogger(__name__)

CHUNK_BYTES = 1024 * 1024
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


class DownloadError(Exception):
    """The file could not be fetched: a non-retryable answer, or retries were exhausted."""


class _TransientError(Exception):
    """Internal signal: this failure is worth another attempt."""


_RETRY_ON = (
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
    _TransientError,
)


@dataclass(frozen=True)
class RemoteInfo:
    """What the source says about a file, learned without downloading it."""

    status: int
    etag: str | None
    last_modified: str | None
    content_length: int | None


@dataclass(frozen=True)
class DownloadResult:
    size_bytes: int
    sha256: str
    etag: str | None  # taken from the GET that delivered these bytes, not from an earlier HEAD
    last_modified: str | None


def _optional_int(value: str | None) -> int | None:
    return int(value) if value is not None and value.isdigit() else None


class SourceClient:
    def __init__(
        self,
        settings: HttpSettings,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._settings = settings
        self._timeout = (10, settings.timeout_seconds)
        self._sleep = sleep
        self._session = session or requests.Session()
        # identity: no transparent compression, so byte counts match Content-Length exactly
        self._session.headers.update(
            {"User-Agent": settings.user_agent, "Accept-Encoding": "identity"}
        )

    def _with_retry(self, func: Callable, description: str):
        return retry_call(
            func,
            retry_on=_RETRY_ON,
            max_attempts=self._settings.max_attempts,
            base_delay=self._settings.backoff_base_seconds,
            sleep=self._sleep,
            description=description,
        )

    def head(self, url: str) -> RemoteInfo:
        """Ask for headers only. A 403/404 is returned as data, not raised."""

        def attempt() -> RemoteInfo:
            response = self._session.head(url, timeout=self._timeout, allow_redirects=True)
            if response.status_code in RETRYABLE_STATUSES:
                raise _TransientError(f"HTTP {response.status_code}")
            return RemoteInfo(
                status=response.status_code,
                etag=response.headers.get("ETag"),
                last_modified=response.headers.get("Last-Modified"),
                content_length=_optional_int(response.headers.get("Content-Length")),
            )

        try:
            return self._with_retry(attempt, f"HEAD {url}")
        except _RETRY_ON as exc:
            raise DownloadError(f"HEAD {url} failed after retries: {exc}") from exc

    def download(self, url: str, destination: Path) -> DownloadResult:
        """Stream url into destination, hashing on the fly and checking the byte count.

        The caller passes a temporary path and decides when it becomes the real file.
        """

        def attempt() -> DownloadResult:
            with self._session.get(url, stream=True, timeout=self._timeout) as response:
                if response.status_code in RETRYABLE_STATUSES:
                    raise _TransientError(f"HTTP {response.status_code}")
                if response.status_code != 200:
                    raise DownloadError(f"GET {url} returned HTTP {response.status_code}")

                digest = hashlib.sha256()
                written = 0
                with open(destination, "wb") as handle:  # "wb" truncates a failed earlier try
                    for chunk in response.iter_content(chunk_size=CHUNK_BYTES):
                        handle.write(chunk)
                        digest.update(chunk)
                        written += len(chunk)

                expected = _optional_int(response.headers.get("Content-Length"))
                if expected is not None and expected != written:
                    raise _TransientError(f"incomplete download: got {written} of {expected} bytes")

                return DownloadResult(
                    size_bytes=written,
                    sha256=digest.hexdigest(),
                    etag=response.headers.get("ETag"),
                    last_modified=response.headers.get("Last-Modified"),
                )

        try:
            return self._with_retry(attempt, f"GET {url}")
        except _RETRY_ON as exc:
            raise DownloadError(f"GET {url} failed after retries: {exc}") from exc