from __future__ import annotations

from typing import Iterable, Optional, TypeVar
from pyspark import RDD, SparkContext
from pyspark.sql import SparkSession
from utils.logging.factory import LoggerFactory
from utils.config.factory import ConfigFactory
from pyspark.sql import DataFrame


T = TypeVar("T")


class SparkAPI:
    _instance = None

    def __init__(self, session: SparkSession) -> None:
        self._session: SparkSession = session

    @staticmethod
    def get() -> SparkAPI:
        if SparkAPI._instance is None:
            SparkAPI._instance = SparkBuilder.from_default_config().build()
            LoggerFactory.spark_logger().spark_api_loaded()
        return SparkAPI._instance

    @property
    def context(self) -> SparkContext:
        return self._session.sparkContext

    @property
    def session(self) -> SparkSession:
        return self._session

    def parallelize(self, c: Iterable[T], numSlices: Optional[int] = None) -> RDD[T]:
        return self._session.sparkContext.parallelize(c, numSlices)

    def read_parquet_from_hdfs(self) -> DataFrame:
        config = ConfigFactory.config()
        return self._session.read.parquet(config.hdfs_dataset_path)


class SparkBuilder:
    """Builds the Spark API."""

    def __init__(self, master: str, app_name: str, port: int):
        self._master_url = "spark://" + master + ":" + str(port)
        self._app_name = app_name

    @staticmethod
    def from_default_config() -> SparkBuilder:
        config = ConfigFactory.config()
        return SparkBuilder(config.spark_master, config.spark_app_name, config.spark_port)

    def build(self) -> SparkAPI:
        context = SparkContext(self._master_url, self._app_name)
        return SparkAPI(SparkSession(sparkContext=context))
