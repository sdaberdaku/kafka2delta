import logging
from time import sleep
from typing import Generator, Any

import pytest
from confluent_kafka.admin import AdminClient
from psycopg2.extensions import cursor
from pyspark.sql import SparkSession, functions as f

from kafka2delta.config import DeltaTableConfig

logger = logging.getLogger(__name__)


def get_delta_table_name(kafka_topic: str) -> str:
    return kafka_topic.replace(".", "_")


def get_delta_table_qualified_name(database: str, kafka_topic: str) -> str:
    delta_table_name = get_delta_table_name(kafka_topic)
    return f"{database}.{delta_table_name}"


def get_delta_table_path(destination_s3_bucket: str, delta_tables_base_path: str, kafka_topic: str) -> str:
    delta_table_name = get_delta_table_name(kafka_topic)
    return f"s3a://{destination_s3_bucket}/{delta_tables_base_path}/{delta_table_name}"


@pytest.mark.parametrize("table", [
    dict(schema="public", name="orders", exists=True),
    dict(schema="public", name="products", exists=True),
    dict(schema="public", name="users", exists=True),
    dict(schema="test", name="not_exists", exists=False),
])
def test_postgres_tables_exist(pg_cursor: cursor, table: dict[str, Any]) -> None:
    pg_cursor.execute(f"""
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_type='BASE TABLE'
        AND table_schema = '{table["schema"]}'
        AND table_name = '{table["name"]}'
    );
    """)
    exists = pg_cursor.fetchone()[0]
    assert exists == table["exists"], (f"Expected {table['schema']}.{table['name']} to "
                                       f"{'exist' if table['exists'] else 'not exist'}!")


@pytest.fixture(scope="session")
def kafka_topics() -> list[str]:
    return [
        "postgres.public.orders",
        "postgres.public.products",
        "postgres.public.users",
    ]


def test_kafka_topics_exist(kafka_client: AdminClient, kafka_topics: list[str]) -> None:
    existing_topics = kafka_client.list_topics().topics.keys()
    for topic in kafka_topics:
        assert topic in existing_topics, f"Expected topic {topic} to be in {existing_topics}!"


@pytest.fixture(scope="session")
def database(spark: SparkSession) -> str:
    database = "test_database"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    return database


@pytest.fixture(scope="session")
def delta_table_configs(s3_bucket: str, database: str, kafka_topics: list[str]) -> dict[str, DeltaTableConfig]:
    delta_tables_base_path = "delta_tables"
    return {
        kafka_topic: DeltaTableConfig(
            qualified_name=get_delta_table_qualified_name(database, kafka_topic),
            path=get_delta_table_path(s3_bucket, delta_tables_base_path, kafka_topic),
            additional_cols=[f.year("created_at").alias("year"), f.month("created_at").alias("month")],
            partition_cols=["year", "month"],
        ) for kafka_topic in kafka_topics
    }


@pytest.fixture
def streaming_query(
        add_users: int,
        kafka_topics: list[str],
        delta_table_configs: dict[str, DeltaTableConfig],
        kafka_bootstrap_server_url: str,
        schema_registry_url: str,
        s3_bucket: str,
        spark: SparkSession,
) -> Generator[str, None, None]:
    query_name = "test_streaming_query"
    checkpoints_base_path = "checkpoints"
    kafka_bootstrap_server_url = kafka_bootstrap_server_url
    schema_registry_url = schema_registry_url

    # register UDFs, only after starting Spark Session!
    from kafka2delta.udf import get_schema_id, get_confluent_avro_value
    spark.udf.register("get_confluent_avro_schema", get_confluent_avro_value)
    spark.udf.register("get_schema_version", get_schema_id)

    checkpoints_path = f"s3a://{s3_bucket}/{checkpoints_base_path}"

    from kafka2delta.stream import stream_to_delta
    stream_to_delta(
        spark=spark,
        query_name=query_name,
        kafka_bootstrap_server_url=kafka_bootstrap_server_url,
        kafka_topics=kafka_topics,
        delta_table_configs=delta_table_configs,
        checkpoints_path=checkpoints_path,
        schema_registry_url=schema_registry_url,
        processing_time="1 seconds"
    )

    yield query_name

    for query in spark.streams.active:
        logger.info(f"Name: {query.name}, ID: {query.id}, Is Active: {query.isActive}")
        query.stop()
        logger.info(f"Stopped query: {query.name}")


def test_single_streaming_query(spark: SparkSession, streaming_query: str) -> None:
    assert len(spark.streams.active) == 1, "Expected 1 active streaming query!"


def test_start_streaming(spark: SparkSession, streaming_query: str) -> None:
    active_queries = [q.name for q in spark.streams.active]
    assert streaming_query in active_queries, f"Expected {streaming_query} to be in {active_queries}!"


@pytest.fixture
def add_users(pg_cursor: cursor) -> int:
    pg_cursor.execute("""
    INSERT INTO public.users (name, email, created_at) VALUES
    ('Alice Johnson', 'alice.johnson@example.com', '2024-01-01'),
    ('Bob Smith', 'bob.smith@example.com', '2025-01-02'),
    ('Charlie Brown', 'charlie.brown@example.com', '2023-01-03'),
    ('David White', 'david.white@example.com', '2024-02-04'),
    ('Emma Green', 'emma.green@example.com', '2024-03-05'),
    ('Frank Black', 'frank.black@example.com', '2024-04-06'),
    ('Grace Hall', 'grace.hall@example.com', '2025-02-07'),
    ('Henry Adams', 'henry.adams@example.com', '2024-02-08'),
    ('Isabella Lewis', 'isabella.lewis@example.com', '2024-11-09'),
    ('Jack Miller', 'jack.miller@example.com', '2024-12-10')
    ON CONFLICT (email) DO NOTHING;
    """)
    sleep(60)
    return 10


def test_delta_table_creation_after_postgres_insert(
        spark: SparkSession,
        streaming_query: str,
        add_users: int,
        delta_table_configs: dict[str, DeltaTableConfig]
) -> None:
    sleep(60)
    from delta.tables import DeltaTable
    users_delta_table_config = delta_table_configs["postgres.public.users"]
    exists = DeltaTable.isDeltaTable(spark, users_delta_table_config.path)
    assert exists, "Expected Delta Table to already exist!"

    users_df = spark.read.format("delta").load(users_delta_table_config.path)
    n_records = users_df.count()
    assert add_users == n_records, f"Expected {add_users} records, found {n_records} instead!"
