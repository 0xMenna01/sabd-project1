from __future__ import annotations

from typing import Iterable, Optional, TypeVar
from pyspark import RDD, SparkContext
from pyspark.sql import SparkSession
from utils.logging.factory import LoggerFactory
from utils.config.factory import ConfigFactory
from pyspark.sql import DataFrame
from spark.model import DataFormat


T = TypeVar("T")


class SparkAPI:
    _instance = None

    def __init__(self, session: SparkSession) -> None:
        self._session: SparkSession = session

    @staticmethod
    def get() -> SparkAPI:
        if SparkAPI._instance is None:
            SparkAPI._instance = SparkBuilder.from_default_config().build()
            LoggerFactory.spark().spark_api_loaded()
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
        return self._session.read.parquet(config.hdfs_dataset_path + "/dataset.parquet")

    def read_csv_from_hdfs(self) -> DataFrame:
        config = ConfigFactory.config()
        return self._session.read.csv(config.hdfs_dataset_path + "/dataset.csv", header=True)

    def read_json_from_hdfs(self) -> DataFrame:
        config = ConfigFactory.config()
        return self._session.read.json(config.hdfs_dataset_path + "/dataset.json")

    def read_avro_from_hdfs(self) -> DataFrame:
        config = ConfigFactory.config()
        return self._session.read.format("avro").load(config.hdfs_dataset_path + "/dataset.avro")

    def read_from_hdfs(self, data_format: DataFormat) -> DataFrame:
        """Reads the dataset from HDFS in the specified format."""

        if data_format == DataFormat.PARQUET:
            return self.read_parquet_from_hdfs()
        elif data_format == DataFormat.CSV:
            return self.read_csv_from_hdfs()
        elif data_format == DataFormat.JSON:
            return self.read_json_from_hdfs()
        else:
            return self.read_avro_from_hdfs()

    def write_results_to_hdfs_as_csv(self, df: DataFrame, filename: str) -> None:
        config = ConfigFactory.config()
        df.write.csv(
            config.hdfs_results_path + "/" + filename,
            mode="overwrite",
            header=True
        )


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
