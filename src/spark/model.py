from enum import Enum
from typing import List


class DataFormat(Enum):
    """Format of the data to read."""
    PARQUET = 1
    CSV = 2
    AVRO = 3


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


class SparkActionResult:
    """Result of a Spark job."""

    def __init__(self, name: str, header: list[str], sort_list: list, result: list, execution_time: float, ascending_list: list | None = None):
        self._name = name
        self._header = header
        self._sort_list = sort_list
        self._ascending_list = ascending_list
        self._result = result
        self._execution_time = execution_time

    @property
    def name(self) -> str:
        return self._name

    @property
    def header(self) -> list[str]:
        return self._header

    @property
    def sort_list(self) -> list:
        return self._sort_list

    @property
    def ascending_list(self) -> list | None:
        return self._ascending_list

    @property
    def result(self) -> list:
        return self._result

    @property
    def exec_time(self) -> float:
        return self._execution_time


class QueryResult(list[SparkActionResult]):
    """List of Spark job results."""

    def __init__(self, name: str, results: List[SparkActionResult]):
        super().__init__(results)
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def total_exec_time(self) -> float:
        return sum([res.exec_time for res in self])


class SparkError(Exception):
    """Custom exception for Spark errors"""
    pass
