from typing import Callable, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery
    from kafka2delta.config import DeltaTableConfig


def merge_micro_batch(
        schema_registry_url: str,
        delta_table_configs: dict[str, "DeltaTableConfig"],
        avro_options: dict = None,
        lsn_col_name: str = "__log_sequence_number",
        deleted_col_name: str = "__deleted"
) -> Callable[["DataFrame", int], None]:
    """
    Returns a function used for merging Structured Streaming micro-batches.

    :param schema_registry_url: Confluent Schema Registry URL.
    :param delta_table_configs: Dict providing the delta table configurations for each topic.
    :param avro_options: Options for decoding from Avro. Defaults to {"mode": "FAILFAST"}.
    :param lsn_col_name: Name of the column containing the log sequence number. Defaults to "__log_sequence_number".
    :param deleted_col_name: Name of the column indicating if the record has been deleted. Defaults to "__deleted".
    :return: A function used for merging micro-batches.
    """
    from pyspark.sql import functions as f, types as t, Window
    from pyspark.sql.avro.functions import from_avro

    from kafka2delta.utils import (
        cast_debezium_columns,
        create_delta_table_if_not_exist,
        get_json_schema,
        get_column_names_from_schema,
    )

    if avro_options is None:
        avro_options = {"mode": "FAILFAST"}

    def func(batch_df: "DataFrame", batch_id: int) -> None:
        """
        Function that parses each micro-batch.

        :param batch_df: The current micro-batch DataFrame.
        :param batch_id: The current micro-batch ID.
        :return: None
        """

        # Caching the micro-batch to avoid reading it multiple times.
        batch_df = batch_df.cache()
        try:
            kafka_topics = batch_df.select("topic").distinct().collect()
            for topic, in kafka_topics:
                # Getting the Delta Table qualified name and path for the current topic.
                dt_config = delta_table_configs[topic]
                source = topic.replace(".", "_")
                # Filtering the records of the current topic and caching the result.
                current_batch_df = batch_df.filter(f.col("topic") == f.lit(topic)).cache()
                try:
                    # For each distinct key/value schema version combination.
                    # The Confluent Schema Registry assigns globally unique IDs to each registered schema.
                    # Allocated IDs are guaranteed to be monotonically increasing and unique, but not necessarily
                    # consecutive.
                    schema_version_pairs = (current_batch_df
                                            .select("key_schema_id", "value_schema_id")
                                            .distinct()
                                            .sort(["value_schema_id", "key_schema_id"], ascending=True)
                                            .collect())
                    for ks_id, vs_id in schema_version_pairs:
                        # Determine the primary key columns.
                        primary_key_cols = get_column_names_from_schema(
                            schema_registry_url=schema_registry_url,
                            schema_id=ks_id
                        )
                        # Fetch the value schema, the Confluent Schema Registry client caches results.
                        value_schema = get_json_schema(
                            schema_registry_url=schema_registry_url,
                            schema_id=vs_id
                        )
                        # Filter the records corresponding to this particular schema version.
                        filtered_df = (current_batch_df
                                       .filter(f.col("key_schema_id") == f.lit(ks_id))
                                       .filter(f.col("value_schema_id") == f.lit(vs_id))
                                       )
                        # Decode the value field from AVRO and cast to appropriate column types.
                        decoded_df = (filtered_df
                                      .select(from_avro(f.col("value_avro"), value_schema, avro_options).alias("value"))
                                      .select("value.*")
                                      .select(cast_debezium_columns(avro_json_schema=value_schema))
                                      )

                        # For each primary key, we are only interested in the latest version/change.
                        w = Window.partitionBy(primary_key_cols).orderBy(f.desc(lsn_col_name))
                        source_df = (decoded_df
                                     .withColumn("__row_number", f.row_number().over(w))
                                     .filter(f.col("__row_number") == f.lit(1))
                                     .drop("__row_number"))

                        # Add additional columns.
                        if dt_config.additional_cols:
                            source_df = source_df.select(
                                *source_df.columns,
                                *[f.expr(a) for a in dt_config.additional_cols]
                            )

                        # Filter out "deleted" column.
                        target_schema = t.StructType([c for c in source_df.schema.fields if c.name != deleted_col_name])
                        # Create an empty target Delta Table if it does not already exist.
                        create_delta_table_if_not_exist(
                            spark=source_df.sparkSession,
                            schema=target_schema,
                            config=dt_config
                        )

                        # Perform the upsert operation.
                        upserted_records_df = source_df.filter(f.col(deleted_col_name) != f.lit('true'))
                        merge_cols = primary_key_cols + dt_config.partition_cols
                        merge_condition = " AND ".join(f"target.{c} = {source}.{c}" for c in merge_cols)
                        update = ", ".join(f"{c} = {source}.{c}" for c in target_schema.names)
                        insert_cols = ", ".join(target_schema.names)
                        insert_values = ", ".join(f"{source}.{c}" for c in target_schema.names)
                        upsert_query = f"""
                            MERGE INTO delta.`{dt_config.path}` AS target
                            USING {source}
                              ON {merge_condition}
                            WHEN MATCHED AND target.{lsn_col_name} < {source}.{lsn_col_name} THEN
                              UPDATE SET {update}
                            WHEN NOT MATCHED THEN 
                              INSERT ({insert_cols}) VALUES ({insert_values})
                        """
                        # Create/replace temp view for the current stream.
                        upserted_records_df.createOrReplaceTempView(name=source)
                        # Run the MERGE query.
                        upserted_records_df.sparkSession.sql(upsert_query)

                        # Perform the delete operation.
                        deleted_records_df = source_df.filter(f.col(deleted_col_name) == f.lit('true'))
                        delete_condition = " AND ".join(f"target.{c} = {source}.{c}" for c in primary_key_cols)
                        delete_query = f"""
                            MERGE INTO delta.`{dt_config.path}` AS target
                            USING {source}
                              ON {delete_condition}
                            WHEN MATCHED THEN 
                              DELETE
                        """
                        # Create/replace temp view for the current stream.
                        deleted_records_df.createOrReplaceTempView(name=source)
                        # Run the MERGE query.
                        deleted_records_df.sparkSession.sql(delete_query)
                finally:
                    # Drop the temp view.
                    current_batch_df.sparkSession.sql(f"DROP VIEW IF EXISTS {source}")
                    # Unpersist the current batch dataframe.
                    current_batch_df.unpersist()
        finally:
            # Unpersist the micro-batch dataframe.
            batch_df.unpersist()

    return func


def stream_to_delta(
        spark: "SparkSession",
        query_name: str,
        kafka_bootstrap_server_url: str,
        kafka_topics: list[str],
        delta_table_configs: dict[str, "DeltaTableConfig"],
        checkpoints_path: str,
        schema_registry_url: str,
        starting_offset: Literal["earliest", "latest"] = "earliest",
        fail_on_data_loss: bool = True,
        processing_time: str | None = "0 seconds",
) -> "StreamingQuery":
    """
    Starts a Spark Structured Streaming Query that reads messages from the provided Kafka topics and merges it into
    target Delta Tables

    :param spark: Spark Session object.
    :param query_name: The query name.
    :param kafka_bootstrap_server_url: Kafka bootstrap server URL.
    :param kafka_topics: List of Kafka topics to stream from.
    :param delta_table_configs: Dict providing the target delta table configurations for each topic.
    :param checkpoints_path: The destination checkpoints path.
    :param schema_registry_url: Confluent Schema Registry URL.
    :param starting_offset: If set to "earliest", the stream will start reading data from the earliest available offset
        in the topic. If set to "latest", the stream will only start reading data from the time that it started reading
        and not pull anything older from the topic.
    :param fail_on_data_loss: Stop the stream if there is a break in the sequence of offsets.
    :param processing_time: The processing time interval as a string, e.g. '5 seconds', '1 minute'. Set a trigger that
        runs a micro-batch query periodically based on the processing time. If this is not set, it will run the query as
        fast as possible, which is equivalent to setting the trigger to ``processingTime='0 seconds'``.
    :return: The newly created StreamingQuery object.
    :raises StreamingQueryException: If the streaming query fails to start.
    """
    from pyspark.sql.streaming import StreamingQueryException

    df = (spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", kafka_bootstrap_server_url)
          .option("subscribe", ",".join(kafka_topics))
          .option("startingOffsets", starting_offset)
          .option("failOnDataLoss", fail_on_data_loss)
          .load()
          )

    # Must be imported after the Spark Session has started!
    from kafka2delta.udf import get_schema_id, get_confluent_avro_value

    df = df.select(
        "topic",
        "partition",
        "offset",
        "timestamp",
        "timestampType",
        get_schema_id("key").alias("key_schema_id"),
        get_schema_id("value").alias("value_schema_id"),
        get_confluent_avro_value("value").alias("value_avro")
    )

    parser = merge_micro_batch(schema_registry_url=schema_registry_url, delta_table_configs=delta_table_configs)

    streaming_query = (
        df
        .writeStream
        .queryName(queryName=query_name)
        .format("delta")
        .option("checkpointLocation", checkpoints_path)
        .outputMode("update")
        .trigger(processingTime=processing_time)
        .foreachBatch(parser)
        .start()
    )
    if not streaming_query.isActive:
        raise StreamingQueryException(f"Streaming query '{query_name}' failed to start!")
    return streaming_query
