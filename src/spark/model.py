from enum import Enum


class DataFormat(Enum):
    """Format of the data to read."""
    PARQUET = 1
    CSV = 2
    JSON = 3
    AVRO = 4


class QueryFramework(Enum):
    """Framework to use for executing queries."""
    SPARK_CORE = 1
    SPARK_SQL = 2
    SPARK_CORE_AND_SQL = 3


class QueryNum(Enum):
    """Specific query to execute."""
    QUERY_ONE = 1
    QUERY_TWO = 2
    QUERY_THREE = 3
    QUERY_ALL = 4


class SparkError(Exception):
    """Custom exception for Spark errors"""
    pass
