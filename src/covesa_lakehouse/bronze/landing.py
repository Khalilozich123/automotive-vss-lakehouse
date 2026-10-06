"""The raw landing zone: an S3 prefix of immutable JSONL files, one record per line.

Line format (the same for the MQTT bridge and for batch file drops)::

    {"topic": "vehicles/<platform_id>/<vin>/telemetry", "payload": "<raw payload>",
     "received_at": "2026-09-01T07:00:00.123Z"}           # received_at optional

Layout::

    s3://landing/telemetry/<source>/dt=YYYY-MM-DD/hour=HH/part-<utc>-<writer>-<seq>.jsonl

Upload a file drop (e.g. a simulator backfill)::

    uv run python -m covesa_lakehouse.bronze.landing upload data/landing/day1.jsonl
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

Source = Literal["mqtt", "batch"]
DEFAULT_BUCKET = "landing"
PREFIX = "telemetry"


def object_key(source: Source, when: datetime, writer_id: str, seq: int) -> str:
    when = when.astimezone(UTC)
    return (
        f"{PREFIX}/{source}/dt={when:%Y-%m-%d}/hour={when:%H}/"
        f"part-{when:%Y%m%dT%H%M%S}-{writer_id}-{seq:06d}.jsonl"
    )


class ObjectWriter(Protocol):
    def put(self, key: str, data: bytes) -> None: ...


class S3Landing:
    """Writes landing objects. Each S3 PUT is atomic, so readers never see partial files."""

    def __init__(self, bucket: str = DEFAULT_BUCKET, client: S3Client | None = None) -> None:
        self.bucket = bucket
        self.client: S3Client = client or boto3.client(
            "s3",
            endpoint_url=os.environ.get("LAKEHOUSE_S3_ENDPOINT", "http://rustfs:9000"),
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
        )

    def put(self, key: str, data: bytes) -> None:
        self.client.put_object(
            Bucket=self.bucket, Key=key, Body=data, ContentType="application/x-ndjson"
        )

    def upload_file(self, path: Path, source: Source = "batch") -> str:
        now = datetime.now(UTC)
        key = (
            f"{PREFIX}/{source}/dt={now:%Y-%m-%d}/hour={now:%H}/"
            f"drop-{now:%Y%m%dT%H%M%S}-{path.stem}.jsonl"
        )
        self.client.upload_file(str(path), self.bucket, key)
        return f"s3://{self.bucket}/{key}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    up = sub.add_parser("upload", help="upload JSONL file drops into the landing zone")
    up.add_argument("files", nargs="+", type=Path)
    up.add_argument("--bucket", default=DEFAULT_BUCKET)
    args = parser.parse_args()

    landing = S3Landing(args.bucket)
    for path in args.files:
        print(landing.upload_file(path))


if __name__ == "__main__":
    main()
