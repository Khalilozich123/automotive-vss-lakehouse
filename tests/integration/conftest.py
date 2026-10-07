import os
from collections.abc import Callable

import boto3
import pytest


@pytest.fixture
def delete_s3_prefix() -> Callable[[str, str], None]:
    """Remove a test's private landing/checkpoint objects (tables are dropped separately)."""
    s3 = boto3.resource(
        "s3",
        endpoint_url=os.environ.get("LAKEHOUSE_S3_ENDPOINT", "http://rustfs:9000"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )

    def delete(bucket: str, prefix: str) -> None:
        s3.Bucket(bucket).objects.filter(Prefix=prefix).delete()

    return delete
