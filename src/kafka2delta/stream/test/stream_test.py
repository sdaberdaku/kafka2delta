import logging
import uuid
from time import time
from typing import Generator, Any

import pytest
from confluent_kafka.admin import AdminClient
from psycopg2.extensions import cursor
from pyspark.sql import SparkSession

from kafka2delta.config import DeltaTableConfig
from kafka2delta.stream.test.stream_listener import BatchProcessingListener

logger = logging.getLogger(__name__)


def get_delta_table_name(kafka_topic: str) -> str:
    return kafka_topic.replace(".", "_")


def get_delta_table_path(destination_s3_bucket: str, delta_tables_base_path: str, kafka_topic: str) -> str:
    delta_table_name = get_delta_table_name(kafka_topic)
    return f"s3a://{destination_s3_bucket}/{delta_tables_base_path}/{delta_table_name}"


def generate_unique_test_id() -> str:
    return f"test-{int(time())}-{str(uuid.uuid4()).split('-')[0]}"


def execute_and_wait_for_streaming_query(
        streaming_query_listener: BatchProcessingListener,
        sql_query: str,
        expected_rows_count: int,
        pg_cursor: cursor,
) -> None:
    """
    Execute a PostgreSQL query and wait for the streaming process to handle it.

    Args:
        streaming_query_listener: The listener tracking batch processing
        sql_query: The SQL query to execute
        expected_rows_count: Number of rows expected to be processed
        pg_cursor: PostgreSQL cursor to execute the query

    Raises:
        AssertionError: If the operation doesn't process the expected number of rows
    """
    streaming_query_listener.set_checkpoint()
    pg_cursor.execute(sql_query)
    success = streaming_query_listener.wait_for_rows(
        expected_rows_count=expected_rows_count
    )
    assert success, f"Failed to process {expected_rows_count} rows" \
                    f" error: {streaming_query_listener.error_message}"


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
            schema=database,
            table_name=get_delta_table_name(kafka_topic),
            path=get_delta_table_path(s3_bucket, delta_tables_base_path, kafka_topic),
            additional_cols=["YEAR(created_at) AS year", "MONTH(created_at) AS month"],
            partition_cols=["year", "month"],
        ) for kafka_topic in kafka_topics
    }


@pytest.fixture
def streaming_query(
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


def test_delta_table_insertion_after_postgres_insert(
        spark: SparkSession,
        streaming_query: str,
        streaming_query_listener: BatchProcessingListener,
        delta_table_configs: dict[str, DeltaTableConfig],
        pg_cursor: cursor,
) -> None:
    # Wait for initial snapshot to complete by detecting idle state of streaming query
    snapshot_complete = streaming_query_listener.wait_for_snapshot_to_complete()
    assert snapshot_complete, "Initial snapshot did not complete - query never became idle"

    test_id = generate_unique_test_id()

    # Insert test data and wait for processing
    num_of_rows = 10
    execute_and_wait_for_streaming_query(
        streaming_query_listener=streaming_query_listener,
        sql_query=f"""
            INSERT INTO public.users (name, email, created_at) VALUES
            ('Alice Johnson', 'alice.{test_id}@example.com', '2024-01-01'),
            ('Bob Smith', 'bob.{test_id}@example.com', '2025-01-02'),
            ('Charlie Brown', 'charlie.{test_id}@example.com', '2023-01-03'),
            ('David White', 'david.{test_id}@example.com', '2024-02-04'),
            ('Emma Green', 'emma.{test_id}@example.com', '2024-03-05'),
            ('Frank Black', 'frank.{test_id}@example.com', '2024-04-06'),
            ('Grace Hall', 'grace.{test_id}@example.com', '2025-02-07'),
            ('Henry Adams', 'henry.{test_id}@example.com', '2024-02-08'),
            ('Isabella Lewis', 'isabella.{test_id}@example.com', '2024-11-09'),
            ('Jack Miller', 'jack.{test_id}@example.com', '2024-12-10')
            ON CONFLICT (email) DO NOTHING;
        """,
        expected_rows_count=num_of_rows,
        pg_cursor=pg_cursor,
    )

    # Verify Delta table exists and has our data
    from delta.tables import DeltaTable
    users_delta_path = delta_table_configs["postgres.public.users"].path
    assert DeltaTable.isDeltaTable(spark, users_delta_path), "Delta table does not exist"

    # Quick check that at least one test record exists
    users_df = spark.read.format("delta").load(users_delta_path)
    test_records = users_df.filter(f"email like '%{test_id}%'")

    actual_count = test_records.count()
    assert actual_count == num_of_rows, f"Expected {num_of_rows} test records, found {actual_count}"


def test_delta_table_update_after_postgres_update(
        spark: SparkSession,
        streaming_query: str,
        streaming_query_listener: BatchProcessingListener,
        delta_table_configs: dict[str, DeltaTableConfig],
        pg_cursor: cursor,
) -> None:
    # Wait for initial snapshot to complete by detecting idle state of streaming query
    snapshot_complete = streaming_query_listener.wait_for_snapshot_to_complete()
    assert snapshot_complete, "Initial snapshot did not complete - query never became idle"

    test_id = generate_unique_test_id()

    # Insert initial test data
    num_of_initial_rows = 5
    execute_and_wait_for_streaming_query(
        streaming_query_listener=streaming_query_listener,
        sql_query=f"""
            INSERT INTO public.users (name, email, created_at) VALUES
            ('Alice Johnson', 'alice.{test_id}@example.com', '2024-12-31'),
            ('Bob Smith', 'bob.{test_id}@example.com', '2025-01-02'),
            ('Charlie Brown', 'charlie.{test_id}@example.com', '2024-12-31'),
            ('David White', 'david.{test_id}@example.com', '2024-12-31'),
            ('Emma Green', 'emma.{test_id}@example.com', '2024-03-05')
            ON CONFLICT (email) DO NOTHING;
        """,
        expected_rows_count=num_of_initial_rows,
        pg_cursor=pg_cursor,
    )

    # Perform update operation
    num_of_updated_rows = 3
    execute_and_wait_for_streaming_query(
        streaming_query_listener=streaming_query_listener,
        sql_query=f"""
            UPDATE public.users
            SET
                name = name || ' (Updated)'
            WHERE email LIKE '%{test_id}%'
            AND created_at = '2024-12-31';
        """,
        expected_rows_count=num_of_updated_rows,
        pg_cursor=pg_cursor,
    )

    # Read the updated Delta table
    users_df = spark.read.format("delta").load(delta_table_configs["postgres.public.users"].path)
    test_users_df = users_df.filter(f"email like '%{test_id}%'")
    actual_count = test_users_df.count()
    assert actual_count == num_of_initial_rows, f"Expected {num_of_initial_rows} test records, found {actual_count}"

    # Verify updated rows
    updated_records = test_users_df.filter("name like '%Updated%'")
    assert updated_records.count() == num_of_updated_rows, "Not all records were updated correctly"


def test_delta_table_deletion_after_postgres_delete(
        spark: SparkSession,
        streaming_query: str,
        streaming_query_listener: BatchProcessingListener,
        delta_table_configs: dict[str, DeltaTableConfig],
        pg_cursor: cursor,
) -> None:
    # Wait for initial snapshot to complete by detecting idle state of streaming query
    snapshot_complete = streaming_query_listener.wait_for_snapshot_to_complete()
    assert snapshot_complete, "Initial snapshot did not complete - query never became idle"

    test_id = generate_unique_test_id()

    # Insert initial test data
    num_of_initial_rows = 5
    execute_and_wait_for_streaming_query(
        streaming_query_listener=streaming_query_listener,
        sql_query=f"""
            INSERT INTO public.users (name, email, created_at) VALUES
            ('Alice Johnson', 'alice.{test_id}@example.com', '2024-12-31'),
            ('Bob Smith', 'bob.{test_id}@example.com', '2025-01-02'),
            ('Charlie Brown', 'charlie.{test_id}@example.com', '2024-12-31'),
            ('David White', 'david.{test_id}@example.com', '2024-12-31'),
            ('Emma Green', 'emma.{test_id}@example.com', '2024-03-05')
            ON CONFLICT (email) DO NOTHING;
        """,
        expected_rows_count=num_of_initial_rows,
        pg_cursor=pg_cursor,
    )

    # Perform delete operation
    num_of_deleted_rows = 3
    execute_and_wait_for_streaming_query(
        streaming_query_listener=streaming_query_listener,
        sql_query=f"""
            DELETE FROM public.users
            WHERE email LIKE '%{test_id}%'
            AND created_at = '2024-12-31';
        """,
        expected_rows_count=num_of_deleted_rows,
        pg_cursor=pg_cursor,
    )

    # Read the updated Delta table
    users_df = spark.read.format("delta").load(delta_table_configs["postgres.public.users"].path)
    test_users_df = users_df.filter(f"email like '%{test_id}%'")

    # Verify remaining records
    actual_count = test_users_df.count()
    expected_remaining_rows = num_of_initial_rows - num_of_deleted_rows
    assert actual_count == expected_remaining_rows, \
        f"Expected {expected_remaining_rows} test records after deletion, found {actual_count}"

    # Verify specific records remain
    remaining_records = test_users_df.filter("created_at != '2024-12-31'")
    assert remaining_records.count() == 2, "Incorrect number of records remained after deletion"


def test_delta_table_schema_change_adding_column(
        spark: SparkSession,
        streaming_query: str,
        streaming_query_listener: BatchProcessingListener,
        delta_table_configs: dict[str, DeltaTableConfig],
        pg_cursor: cursor,
) -> None:
    # Wait for initial snapshot to complete by detecting idle state of streaming query
    snapshot_complete = streaming_query_listener.wait_for_snapshot_to_complete()
    assert snapshot_complete, "Initial snapshot did not complete - query never became idle"

    test_id = generate_unique_test_id()

    # Insert initial test data
    num_of_initial_rows = 5
    execute_and_wait_for_streaming_query(
        streaming_query_listener=streaming_query_listener,
        sql_query=f"""
            INSERT INTO public.users (name, email, created_at) VALUES
            ('Alice Johnson', 'alice.{test_id}@example.com', '2024-12-31'),
            ('Bob Smith', 'bob.{test_id}@example.com', '2025-01-02'),
            ('Charlie Brown', 'charlie.{test_id}@example.com', '2024-12-31'),
            ('David White', 'david.{test_id}@example.com', '2024-12-31'),
            ('Emma Green', 'emma.{test_id}@example.com', '2024-03-05')
            ON CONFLICT (email) DO NOTHING;
        """,
        expected_rows_count=num_of_initial_rows,
        pg_cursor=pg_cursor,
    )

    # Alter the table to add a new column
    pg_cursor.execute("ALTER TABLE public.users ADD COLUMN phone_number VARCHAR;")
    logger.info("Added new column 'phone_number' to the table")

    # Insert new data with the new column
    num_of_new_rows = 3
    execute_and_wait_for_streaming_query(
        streaming_query_listener=streaming_query_listener,
        sql_query=f"""
            INSERT INTO public.users (name, email, created_at, phone_number) VALUES
            ('Frank Black', 'frank.{test_id}@example.com', '2024-04-06', '123-456-7890'),
            ('Grace Hall', 'grace.{test_id}@example.com', '2025-02-07', '987-654-3210'),
            ('Henry Adams', 'henry.{test_id}@example.com', '2024-02-08', '555-555-5555')
            ON CONFLICT (email) DO NOTHING;
        """,
        expected_rows_count=num_of_new_rows,
        pg_cursor=pg_cursor,
    )

    # Read the updated Delta table
    users_df = spark.read.format("delta").load(delta_table_configs["postgres.public.users"].path)
    test_users_df = users_df.filter(f"email like '%{test_id}%'")

    # Verify the total number of rows
    total_rows = num_of_initial_rows + num_of_new_rows
    actual_count = test_users_df.count()
    assert actual_count == total_rows, f"Expected {total_rows} test records, found {actual_count}"

    # Verify the new column exists and contains correct data
    new_column_data = test_users_df.filter("phone_number IS NOT NULL").select("phone_number").collect()
    assert len(new_column_data) == num_of_new_rows, "New column data is missing or incorrect"

    # Verify the existing rows remain unaffected
    existing_rows = test_users_df.filter("phone_number IS NULL")
    assert existing_rows.count() == num_of_initial_rows, "Existing rows were affected by schema change"


def test_delta_table_schema_change_column_deletion(
        spark: SparkSession,
        streaming_query: str,
        streaming_query_listener: BatchProcessingListener,
        delta_table_configs: dict[str, DeltaTableConfig],
        pg_cursor: cursor,
) -> None:
    # Wait for initial snapshot to complete by detecting idle state of streaming query
    snapshot_complete = streaming_query_listener.wait_for_snapshot_to_complete()
    assert snapshot_complete, "Initial snapshot did not complete - query never became idle"

    test_id = generate_unique_test_id()

    # First i need to check if phone number is a column of the table
    pg_cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='users'
        AND column_name='phone_number';
    """)
    column_exists = pg_cursor.fetchone()
    if not column_exists:
        pg_cursor.execute("ALTER TABLE public.users ADD COLUMN phone_number VARCHAR;")

    # Insert initial test data
    num_of_initial_rows = 5
    execute_and_wait_for_streaming_query(
        streaming_query_listener=streaming_query_listener,
        sql_query=f"""
            INSERT INTO public.users (name, email, created_at, phone_number) VALUES
            ('Alice Johnson', 'alice.{test_id}@example.com', '2024-12-31', '123-456-7890'),
            ('Bob Smith', 'bob.{test_id}@example.com', '2025-01-02', '987-654-3210'),
            ('Charlie Brown', 'charlie.{test_id}@example.com', '2024-12-31', '555-555-5555'),
            ('David White', 'david.{test_id}@example.com', '2024-12-31', '111-222-3333'),
            ('Emma Green', 'emma.{test_id}@example.com', '2024-03-05', '444-555-6666')
            ON CONFLICT (email) DO NOTHING;
        """,
        expected_rows_count=num_of_initial_rows,
        pg_cursor=pg_cursor,
    )

    # Alter the table to drop a column
    pg_cursor.execute("ALTER TABLE public.users DROP COLUMN phone_number;")
    logger.info("Dropped column 'phone_number' from the table")

    # Insert new data without the dropped column
    num_of_new_rows = 3
    execute_and_wait_for_streaming_query(
        streaming_query_listener=streaming_query_listener,
        sql_query=f"""
            INSERT INTO public.users (name, email, created_at) VALUES
            ('Frank Black', 'frank.{test_id}@example.com', '2024-04-06'),
            ('Grace Hall', 'grace.{test_id}@example.com', '2025-02-07'),
            ('Henry Adams', 'henry.{test_id}@example.com', '2024-02-08')
            ON CONFLICT (email) DO NOTHING;
        """,
        expected_rows_count=num_of_new_rows,
        pg_cursor=pg_cursor,
    )

    # Read the updated Delta table
    users_df = spark.read.format("delta").load(delta_table_configs["postgres.public.users"].path)
    test_users_df = users_df.filter(f"email like '%{test_id}%'")

    # Verify the dropped column still exists but all values are NULL
    assert "phone_number" in test_users_df.columns, "Dropped column 'phone_number' does not exist in Delta table"
    dropped_column_data = test_users_df.filter("phone_number IS NULL").select("phone_number")
    assert dropped_column_data.count() == num_of_new_rows, "Dropped column data is not NULL for new rows"
