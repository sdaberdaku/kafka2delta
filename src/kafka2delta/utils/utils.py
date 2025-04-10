import json
from typing import TYPE_CHECKING

from kafka2delta.config import DeltaTableConfig

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, Column
    from pyspark.sql.types import StructType


def get_json_schema(schema_registry_url: str, schema_id: int) -> str:
    """
    Returns the schema from the Schema Registry associated with the given schema ID.
    :param schema_registry_url: Schema Registry URL.
    :param schema_id: Schema ID.
    :return: JSON schema associated with the given schema ID.
    """
    from confluent_kafka.schema_registry import SchemaRegistryClient

    with SchemaRegistryClient({'url': schema_registry_url}) as client:
        return client.get_schema(schema_id=schema_id).schema_str


def get_column_names_from_schema(schema_registry_url: str, schema_id: int) -> list[str]:
    """
    Returns the columns names for the schema associated with the given schema ID.
    :param schema_registry_url: Schema Registry URL.
    :param schema_id: Schema ID.
    :return: List of column names for the given schema.
    """
    key_json_schema = get_json_schema(schema_registry_url=schema_registry_url, schema_id=schema_id)
    fields = json.loads(key_json_schema).get("fields", [])
    field_names = [p.get("name") for p in fields]
    return field_names


def create_delta_table_if_not_exist(
        spark: "SparkSession",
        schema: "StructType",
        config: DeltaTableConfig,
) -> None:
    """
    Creates an empty Delta Table with the provided name, location and schema if it does not already exist.
    :param spark: The Spark Session object.
    :param schema: The table Schema.
    :param config: The Delta Table configurations.
    :return:
    """
    if spark.catalog.tableExists(config.qualified_name):
        return
    empty_df = spark.createDataFrame(data=[], schema=schema)
    dt_writer = (empty_df
                 .write
                 .format("delta")
                 .option("path", config.path))
    if config.partition_cols:
        dt_writer = dt_writer.partitionBy(config.partition_cols)
    dt_writer.saveAsTable(config.qualified_name)


def cast_debezium_columns(avro_json_schema: str) -> list["Column"]:
    """
    Converts a JSON-encoded Debezium schema into a list of PySpark Column objects, casting specific fields based on
    the schema's `connect.name` attribute.

    :param avro_json_schema: Avro schema as JSON string.
    :return: List of columns cast to the required types whenever appropriate.
    """
    from pyspark.sql import functions as f, types as t

    avro_dict_schema = json.loads(avro_json_schema)

    cast_types = {
        "io.debezium.time.ZonedTimestamp": t.TimestampType(),
        # "io.debezium.time.ZonedTime": t.StringType(),
    }
    cast_columns = []
    for field in avro_dict_schema.get("fields", []):
        field_name = field["name"]
        field_type = field["type"]
        # Handle cases where field_type is a dict
        if isinstance(field_type, dict):
            connect_name = field_type.get("connect.name")
            spark_type = cast_types.get(connect_name)
            if spark_type:
                cast_columns.append(f.col(field_name).cast(spark_type))
            else:
                cast_columns.append(f.col(field_name))
        # Handle cases where field_type is a list
        elif isinstance(field_type, list):
            for subfield_type in field_type:
                if isinstance(subfield_type, dict):
                    connect_name = subfield_type.get("connect.name")
                    spark_type = cast_types.get(connect_name)
                    if spark_type:
                        cast_columns.append(f.col(field_name).cast(spark_type))
                        break
            else:
                cast_columns.append(f.col(field_name))
        # Handle other cases
        else:
            cast_columns.append(f.col(field_name))
    return cast_columns
