from pyspark.sql import functions as f, types as t

def extract_schema_id(x: bytes) -> int:
    """
    Extracts the schema ID from a binary key.
    The schema ID is extracted from bytes 1 to 4 of the key.
    If the key is None, it returns None.

    :param x: Binary key from which to extract the schema ID.
    :return: The schema ID.
    """
    return int.from_bytes(x[1:5], byteorder="big") if x else None

def extract_avro_value(x: bytes) -> bytes:
    """
    Extracts the Avro value from a binary value.
    The Avro value is extracted from byte 5 onwards.
    If the value is None, it returns an empty byte array.

    :param x: Binary value from which to extract the Avro value.
    :return: The Avro value.
    """
    return x[5:] if x else bytes()


get_schema_id = f.udf(extract_schema_id, t.IntegerType())
"""
UDF to extract the schema ID from a binary key.
The schema ID is extracted from bytes 1 to 4 of the key.
If the key is None, it returns None.
"""

get_confluent_avro_value = f.udf(extract_avro_value, t.BinaryType())
"""
UDF to extract the Avro value from a binary value.
The Avro value is extracted from byte 5 onwards.
If the value is None, it returns an empty byte array.
"""