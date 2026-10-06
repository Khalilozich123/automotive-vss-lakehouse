"""SparkSession factory wired to the Iceberg REST catalog and S3-compatible object storage.

Every job (Bronze, Silver, Gold) gets its session from :func:`get_spark` so catalog and
storage settings live in exactly one place. Settings come from environment variables,
which docker/docker-compose.yml sets for the ``workspace`` container.

Two S3 clients are configured against the same object store:

* Iceberg ``S3FileIO`` (``s3://``) for lakehouse tables;
* Hadoop ``S3A`` (``s3a://``) for plain files, i.e. the raw landing zone and stream checkpoints.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import SparkSession

CATALOG_NAME = "lakehouse"


@dataclass(frozen=True)
class LakehouseConfig:
    spark_master: str = "local[*]"
    app_name: str = "covesa-lakehouse"
    catalog_uri: str = "http://iceberg-rest:8181"
    warehouse: str = "s3://warehouse/"
    s3_endpoint: str = "http://rustfs:9000"
    s3_access_key: str = "lakehouse"
    s3_secret_key: str = "lakehouse-secret"
    s3_region: str = "us-east-1"
    extra_jars_dir: str | None = None
    driver_host: str | None = None
    executor_memory: str = "2g"
    shuffle_partitions: int = 8

    @classmethod
    def from_env(cls, app_name: str = "covesa-lakehouse") -> LakehouseConfig:
        env = os.environ
        return cls(
            spark_master=env.get("SPARK_MASTER_URL", cls.spark_master),
            app_name=app_name,
            catalog_uri=env.get("LAKEHOUSE_CATALOG_URI", cls.catalog_uri),
            warehouse=env.get("LAKEHOUSE_WAREHOUSE", cls.warehouse),
            s3_endpoint=env.get("LAKEHOUSE_S3_ENDPOINT", cls.s3_endpoint),
            s3_access_key=env.get("AWS_ACCESS_KEY_ID", cls.s3_access_key),
            s3_secret_key=env.get("AWS_SECRET_ACCESS_KEY", cls.s3_secret_key),
            s3_region=env.get("AWS_REGION", cls.s3_region),
            extra_jars_dir=env.get("SPARK_EXTRA_JARS_DIR"),
            driver_host=env.get("SPARK_DRIVER_HOST"),
            executor_memory=env.get("SPARK_EXECUTOR_MEMORY", cls.executor_memory),
            shuffle_partitions=int(env.get("SPARK_SHUFFLE_PARTITIONS", cls.shuffle_partitions)),
        )


def spark_conf(cfg: LakehouseConfig) -> dict[str, str]:
    """Spark settings for an Iceberg REST catalog named ``lakehouse`` plus S3A file access."""
    cat = f"spark.sql.catalog.{CATALOG_NAME}"
    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": CATALOG_NAME,
        cat: "org.apache.iceberg.spark.SparkCatalog",
        f"{cat}.type": "rest",
        f"{cat}.uri": cfg.catalog_uri,
        f"{cat}.warehouse": cfg.warehouse,
        f"{cat}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{cat}.s3.endpoint": cfg.s3_endpoint,
        f"{cat}.s3.path-style-access": "true",
        f"{cat}.s3.access-key-id": cfg.s3_access_key,
        f"{cat}.s3.secret-access-key": cfg.s3_secret_key,
        f"{cat}.client.region": cfg.s3_region,
        "spark.hadoop.fs.s3a.endpoint": cfg.s3_endpoint,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(
            cfg.s3_endpoint.startswith("https")
        ).lower(),
        "spark.hadoop.fs.s3a.access.key": cfg.s3_access_key,
        "spark.hadoop.fs.s3a.secret.key": cfg.s3_secret_key,
        "spark.hadoop.fs.s3a.endpoint.region": cfg.s3_region,
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": str(cfg.shuffle_partitions),
        "spark.executor.memory": cfg.executor_memory,
    }
    if cfg.extra_jars_dir:
        # Same path on driver and executors (one image); JVM classpath, not spark.jars, so that
        # Hadoop's FileSystem service loader can find S3A.
        classpath = f"{cfg.extra_jars_dir.rstrip('/')}/*"
        conf["spark.driver.extraClassPath"] = classpath
        conf["spark.executor.extraClassPath"] = classpath
    if cfg.driver_host:
        # Client mode: executors in other containers must be able to call back to the driver.
        conf["spark.driver.host"] = cfg.driver_host
        conf["spark.driver.bindAddress"] = "0.0.0.0"
    return conf


def get_spark(cfg: LakehouseConfig | None = None) -> SparkSession:
    cfg = cfg or LakehouseConfig.from_env()
    builder = SparkSession.builder.master(cfg.spark_master).appName(cfg.app_name)
    for key, value in spark_conf(cfg).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
