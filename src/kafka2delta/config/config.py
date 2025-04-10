from dataclasses import dataclass, field


@dataclass(kw_only=True)
class DeltaTableConfig:
    """
    Configuration for a Delta table in Spark, defining key parameters for table setup.

    Attributes:
        schema: The database schema where the Delta table will be created.
        table_name: The name of the Delta table.
        path: The file system path where the Delta table is stored.
        additional_cols: A list of custom columns expressions to be added to the Delta table schema.
            Default is an empty list, meaning no additional columns are added.
        partition_cols: A list of column names to be used for partitioning the Delta table.
            Default is an empty list, meaning no partitioning will be applied.
    """
    schema: str
    table_name: str
    path: str
    additional_cols: list[str] = field(default_factory=list)
    partition_cols: list[str] = field(default_factory=list)

    @property
    def qualified_name(self) -> str:
        """
        Returns the fully qualified name of the Delta table, including the database name.
        """
        return f"{self.schema}.{self.table_name}"
