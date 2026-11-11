"""Shared test helpers: a tiny local HTTP server that imitates the data source."""

from __future__ import annotations

import io
import threading
from collections import defaultdict
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi.common.config import HttpSettings, Settings, SourceConfig

PERIOD = "2025-01"
FILE_PATH = f"/yellow_tripdata_{PERIOD}.parquet"


def parquet_bytes(rows: int = 100, extra_column: bool = False) -> bytes:
    data = {"VendorID": list(range(rows)), "fare_amount": [float(i) for i in range(rows)]}
    if extra_column:
        data["cbd_congestion_fee"] = [0.75] * rows
    buffer = io.BytesIO()
    pq.write_table(pa.table(data), buffer)
    return buffer.getvalue()


@dataclass
class Served:
    """How the fake source should answer for one path."""

    body: bytes
    etag: str | None = '"v1"'
    last_modified: str | None = "Wed, 01 Jan 2025 00:00:00 GMT"
    status: int = 200
    fail_first_gets: int = 0  # answer the first N GET requests with HTTP 503
    truncate_to: int | None = None  # declare the full length but send only this many bytes


class SourceServer:
    def __init__(self) -> None:
        self.files: dict[str, Served] = {}
        self.get_counts: dict[str, int] = defaultdict(int)
        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self._httpd.server_address[1]}"

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()

    def _handler(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # keep test output quiet
                pass

            def _reply(self, send_body: bool) -> None:
                served = server.files.get(self.path)
                if served is None or served.status != 200:
                    self.send_response(served.status if served else 404)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                if send_body:
                    server.get_counts[self.path] += 1
                    if server.get_counts[self.path] <= served.fail_first_gets:
                        self.send_response(503)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                self.send_response(200)
                self.send_header("Content-Length", str(len(served.body)))
                if served.etag:
                    self.send_header("ETag", served.etag)
                if served.last_modified:
                    self.send_header("Last-Modified", served.last_modified)
                self.end_headers()
                if send_body:
                    self.wfile.write(served.body[: served.truncate_to])  # None means all

            def do_HEAD(self):
                self._reply(send_body=False)

            def do_GET(self):
                self._reply(send_body=True)

        return Handler


@pytest.fixture
def server():
    source_server = SourceServer()
    source_server.start()
    yield source_server
    source_server.stop()


@pytest.fixture
def settings(tmp_path: Path, server: SourceServer) -> Settings:
    return Settings(
        bronze_dir=tmp_path / "bronze",
        http=HttpSettings(max_attempts=3, backoff_base_seconds=0.0, timeout_seconds=5),
        sources={
            "yellow": SourceConfig(
                name="yellow",
                kind="monthly",
                file_format="parquet",
                url_template=f"{server.base_url}/yellow_tripdata_{{period}}.parquet",
            ),
            "zones": SourceConfig(
                name="zones",
                kind="reference",
                file_format="csv",
                url_template=f"{server.base_url}/taxi_zone_lookup.csv",
                required_columns=("LocationID", "Borough"),
            ),
        },
    )