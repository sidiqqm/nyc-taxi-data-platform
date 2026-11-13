from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

from nyc_taxi.ingestion.manifest import read_sidecar

logger = logging.getLogger(__name__)


class UploadError(Exception):
    """A file could not be uploaded to GCS."""


@dataclass(frozen=True)
class UploadResult:
    local_path: Path
    object_key: str
    uploaded: bool  # False when the remote copy was already up to date
    reason: str = ""


class GcsUploader:
    def __init__(self, bucket_name: str, client: storage.Client | None = None) -> None:
        try:
            self._client = client or storage.Client()
        except Exception as exc:  # credentials missing, network, etc.
            raise UploadError(f"cannot create GCS client: {exc}") from exc
        self.bucket_name = bucket_name
        self._bucket = self._client.bucket(bucket_name)

    def upload_with_sidecar(
        self, local_path: Path, object_key: str, *, force: bool = False
    ) -> UploadResult:
        """Upload one file. Skip if the remote SHA256 already matches the local sidecar."""
        if not local_path.is_file():
            raise UploadError(f"local file does not exist: {local_path}")

        meta = read_sidecar(local_path) or {}
        local_sha = meta.get("sha256")
        blob = self._bucket.blob(object_key)

        if not force and local_sha is not None and blob.exists():
            try:
                blob.reload()
            except GoogleCloudError as exc:
                raise UploadError(f"cannot read metadata of gs://{self.bucket_name}/{object_key}: {exc}") from exc
            remote_sha = (blob.metadata or {}).get("sha256")
            if remote_sha == local_sha:
                logger.info("gs://%s/%s already up to date", self.bucket_name, object_key)
                return UploadResult(local_path, object_key, uploaded=False, reason="already up to date")

        try:
            blob.upload_from_filename(str(local_path))
            if local_sha is not None:
                blob.metadata = {
                    "sha256": local_sha,
                    "source_dataset": str(meta.get("dataset") or ""),
                    "source_period": str(meta.get("period") or ""),
                }
                blob.patch()
        except GoogleCloudError as exc:
            raise UploadError(
                f"upload of {local_path} to gs://{self.bucket_name}/{object_key} failed: {exc}"
            ) from exc

        logger.info(
            "uploaded gs://%s/%s (%d bytes)",
            self.bucket_name,
            object_key,
            local_path.stat().st_size,
        )
        return UploadResult(local_path, object_key, uploaded=True)

    def upload_bronze_tree(self, bronze_dir: Path, *, force: bool = False) -> list[UploadResult]:
        """Upload every data file and its sidecar, plus the run logs under _runs/."""
        if not bronze_dir.is_dir():
            raise UploadError(f"bronze directory does not exist: {bronze_dir}")

        results: list[UploadResult] = []
        for path in sorted(bronze_dir.rglob("*")):
            if not path.is_file():
                continue
            if path.name.endswith(".part") or path.name.endswith(".tmp"):
                continue
            relative = path.relative_to(bronze_dir).as_posix()
            # Sidecars are small; always upload to keep metadata in sync.
            # Data files use SHA256 comparison.
            is_sidecar = path.name.endswith(".meta.json")
            results.append(
                self.upload_with_sidecar(path, relative, force=force or is_sidecar)
            )
        return results