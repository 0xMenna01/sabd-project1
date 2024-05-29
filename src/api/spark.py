from __future__ import annotations

from py4j.java_gateway import java_import
from typing import Iterable, Optional, TypeVar
from pyspark import RDD, SparkContext
from pyspark.sql import SparkSession
from utils.logging.factory import LoggerFactory
from utils.config.factory import ConfigFactory
from pyspark.sql import DataFrame
from spark.model import DataFormat, SparkActionResult

T = TypeVar("T")


class SparkAPI:
    _instance = None

    def __init__(self, session: SparkSession, fs) -> None:
        self._session: SparkSession = session
        self._fs = fs

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
    
    def dataset_exists_on_hdfs(self, ext: str) -> bool:
        dataset_url = ConfigFactory.config().hdfs_dataset_dir_url
        hdfs_path= self.context._jvm.Path(dataset_url + "/dataset." + ext)
        return self._fs.exists(hdfs_path)
       

    def read_parquet_from_hdfs(self) -> DataFrame:
        config = ConfigFactory.config()
        return self._session.read.parquet(config.hdfs_dataset_dir_url + "/dataset.parquet")

    def read_csv_from_hdfs(self) -> DataFrame:
        config = ConfigFactory.config()
        return self._session.read.csv(config.hdfs_dataset_dir_url + "/dataset.csv", header=True)

    def read_avro_from_hdfs(self) -> DataFrame:
        config = ConfigFactory.config()
        return self._session.read.format("avro").load(config.hdfs_dataset_dir_url + "/dataset.avro")

    def read_from_hdfs(self, data_format: DataFormat) -> DataFrame:
        """Reads the dataset from HDFS in the specified format."""

        if data_format == DataFormat.PARQUET:
            return self.read_parquet_from_hdfs()
        elif data_format == DataFormat.CSV:
            return self.read_csv_from_hdfs()
        else:
            return self.read_avro_from_hdfs()

    def df_from_action_result(self, action_res: SparkActionResult) -> DataFrame:
        df = SparkAPI.get().session.createDataFrame(
            action_res.result, schema=action_res.header)

        if action_res.ascending_list is None:
            return df.sort(action_res.sort_list).coalesce(1)
        else:
            return df.sort(action_res.sort_list, ascending=action_res.ascending_list).coalesce(1)


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
        session = SparkSession.builder \
            .appName(self._app_name) \
            .master(self._master_url) \
            .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1") \
            .getOrCreate()
       
        sc = session.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.defaultFS", ConfigFactory.config().hdfs_url)
        java_import(sc._jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(sc._jvm, "org.apache.hadoop.fs.Path")
        fs = sc._jvm.FileSystem.get(hadoop_conf)
        
        return SparkAPI(session, fs)
