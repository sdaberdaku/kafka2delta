import base64
import json

import pytest
from pyspark.sql import types as t, functions as f, DataFrame
from pyspark.sql.avro.functions import from_avro

from kafka2delta.utils import cast_debezium_columns
from kafka2delta.utils.test.avro_schema import avro_schema_str, b64_encoded_values


def test_cast_debezium_columns_with_castable_fields(spark):
    schema_str = json.dumps({
        "type": "record",
        "fields": [
            {"name": "created_at", "type": {"type": "string", "connect.name": "io.debezium.time.ZonedTimestamp"}},
            {"name": "updated_at", "type": "string"}
        ]
    })

    columns = cast_debezium_columns(schema_str)

    assert len(columns) == 2
    assert str(columns[0]) == str(f.col("created_at").cast(t.TimestampType()))
    assert str(columns[1]) == str(f.col("updated_at"))


def test_cast_debezium_columns_with_non_castable_fields(spark):
    schema_str = json.dumps({
        "type": "record",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
        ]
    })

    columns = cast_debezium_columns(schema_str)

    assert len(columns) == 2
    assert str(columns[0]) == str(f.col("id"))
    assert str(columns[1]) == str(f.col("name"))


def test_cast_debezium_columns_with_mixed_fields(spark):
    schema_str = json.dumps({
        "type": "record",
        "fields": [
            {"name": "created_at", "type": {"type": "string", "connect.name": "io.debezium.time.ZonedTimestamp"}},
            {"name": "name", "type": "string"}
        ]
    })

    columns = cast_debezium_columns(schema_str)

    assert len(columns) == 2
    assert str(columns[0]) == str(f.col("created_at").cast(t.TimestampType()))
    assert str(columns[1]) == str(f.col("name"))


def test_cast_debezium_columns_with_nested_types(spark):
    schema_str = json.dumps({
        "type": "record",
        "fields": [
            {"name": "timestamp_field", "type": [
                "null", {"type": "string", "connect.name": "io.debezium.time.ZonedTimestamp"}
            ]},
            {"name": "non_castable_field", "type": [
                "null", "string"
            ]}
        ]
    })

    columns = cast_debezium_columns(schema_str)

    assert len(columns) == 2
    assert str(columns[0]) == str(f.col("timestamp_field").cast(t.TimestampType()))
    assert str(columns[1]) == str(f.col("non_castable_field"))


@pytest.fixture(scope="session")
def avro_values_df(spark) -> DataFrame:
    avro_encoded_values = [(base64.b64decode(v),) for v in b64_encoded_values]
    return spark.createDataFrame(avro_encoded_values, schema=["value"])


def test_avro_conversion_poc(spark, avro_values_df):
    from kafka2delta.udf import get_schema_id, get_confluent_avro_value

    df = avro_values_df.select(
        get_schema_id("value").alias("value_schema_id"),
        get_confluent_avro_value("value").alias("value_avro")
    )
    schema_versions_df = df.select("value_schema_id")
    schema_versions_df.show(truncate=False)

    decoded_df = (df
                  .select(from_avro(f.col("value_avro"), avro_schema_str).alias("value"))
                  .select("value.*")
                  .select(cast_debezium_columns(avro_json_schema=avro_schema_str))
                  )
    decoded_df.show(truncate=False)

    assert decoded_df.select("created_at").schema[0].dataType == t.TimestampType()
    assert decoded_df.select("date").schema[0].dataType == t.DateType()
    assert decoded_df.select("drawdown_date").schema[0].dataType == t.DateType()
