import tempfile
from typing import Generator
from uuid import uuid4

import boto3
import psycopg2
import pytest
from confluent_kafka.admin import AdminClient
from mypy_boto3_s3 import S3Client
from psycopg2.extensions import cursor, ISOLATION_LEVEL_AUTOCOMMIT
from pyspark.sql import SparkSession

from kafka2delta.stream.test.stream_listener import BatchProcessingListener

LOCALSTACK_URL = "http://localstack.localstack.svc.cluster.local:4566"
LOCALSTACK_ACCESS_KEY_ID = "test"
LOCALSTACK_SECRET_ACCESS_KEY = "test"


@pytest.fixture(scope="session")
def kafka_bootstrap_server_url() -> str:
    return "cdc-kafka-bootstrap.cdc.svc.cluster.local:9092"


@pytest.fixture(scope="session")
def schema_registry_url() -> str:
    return "http://schema-registry.cdc.svc.cluster.local:8081"


@pytest.fixture(scope="session")
def kafka_client(kafka_bootstrap_server_url: str) -> Generator[AdminClient, None, None]:
    """
    Fixture that sets up a Kafka client.

    :param kafka_bootstrap_server_url: Kafka bootstrap server URL.
    :return: A generator yielding a KafkaContainer object.
    """
    kafka_client = AdminClient({"bootstrap.servers": kafka_bootstrap_server_url})
    yield kafka_client


@pytest.fixture(scope="session")
def s3_client() -> S3Client:
    """
    Fixture that sets up a S3 client.
    :return: The created S3 client.
    """
    return boto3.client(
        service_name="s3",
        endpoint_url=LOCALSTACK_URL,
        aws_access_key_id=LOCALSTACK_ACCESS_KEY_ID,
        aws_secret_access_key=LOCALSTACK_SECRET_ACCESS_KEY
    )


@pytest.fixture(scope="session")
def s3_bucket(s3_client: S3Client, bucket_name_prefix: str = "test-bucket") -> str:
    """
    Fixture that creates an S3 bucket in LocalStack.

    :param s3_client: The S3Client instance.
    :param bucket_name_prefix: The name prefix of the S3 bucket.
    :return: The name of the created S3 bucket.
    """
    bucket_name = f"{bucket_name_prefix}-{str(uuid4())}"
    s3_client.create_bucket(Bucket=bucket_name)
    return bucket_name


@pytest.fixture(scope="session")
def spark(s3_bucket: str) -> Generator[SparkSession, None, None]:
    """
    Fixture that provides a SparkSession object for running tests that require Spark.

    :return: A generator yielding a SparkSession object configured for local testing with LocalStack and S3.
    """

    spark_jars_packages = {
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.spark:spark-avro_2.12:3.5.1",
        "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    }
    with tempfile.TemporaryDirectory() as tempdir:
        spark = (
            SparkSession
            .builder
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.extraJavaOptions", f"-Dderby.system.home='{tempdir}'")
            .config("spark.jars.packages", ",".join(spark_jars_packages))
            .config("spark.hadoop.fs.s3a.endpoint", LOCALSTACK_URL)
            .config("spark.hadoop.fs.s3a.access.key", LOCALSTACK_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", LOCALSTACK_SECRET_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.warehouse.dir", f"{tempdir}/default-warehouse-dir")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .enableHiveSupport()
            .getOrCreate()
        )
        yield spark
        spark.stop()


@pytest.fixture
def pg_cursor() -> Generator[cursor, None, None]:
    """
    Fixture to create and yield a PostgreSQL connection
    :yield: A PostgreSQL cursor
    """
    db_config = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "postgres",
        "host": "postgres-postgresql.postgres.svc.cluster.local",
        "port": "5432"
    }
    conn = psycopg2.connect(**db_config)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # Enable autocommit
    try:
        with conn.cursor() as curs:
            yield curs
    finally:
        conn.close()


@pytest.fixture(scope="session")
def streaming_query_listener(spark: SparkSession) -> Generator[BatchProcessingListener, None, None]:
    """Fixture for baseline-aware streaming query listener"""
    listener = BatchProcessingListener()
    spark.streams.addListener(listener)
    yield listener
    spark.streams.removeListener(listener)
