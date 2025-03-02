from pyspark.sql import functions as f, types as t

get_schema_id = f.udf(lambda x: int.from_bytes(x[1:5], byteorder="big") if x else None, t.IntegerType())

get_confluent_avro_value = f.udf(lambda x: x[5:] if x else bytes(), t.BinaryType())
