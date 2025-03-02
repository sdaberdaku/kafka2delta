from dataclasses import dataclass, field

from pyspark.sql import Column


@dataclass(kw_only=True)
class DeltaTableConfig:
    """
    Configuration for a Delta table in Spark, defining key parameters for table setup.

    Attributes:
        qualified_name: The qualified name of the Delta table, typically including the database or schema name.
        path: The file system path where the Delta table is stored.
        additional_cols: A list of custom columns (as `Column` objects) to be added to the Delta table schema.
            Default is an empty list, meaning no additional columns are added.
        partition_cols: A list of column names to be used for partitioning the Delta table.
            Default is an empty list, meaning no partitioning will be applied.
    """
    qualified_name: str
    path: str
    additional_cols: list[Column] = field(default_factory=list)
    partition_cols: list[str] = field(default_factory=list)
