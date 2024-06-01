from __future__ import annotations
import time

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

    def file_exists_on_hdfs(self, filename: str, ext: str) -> bool:
        dataset_url = ConfigFactory.config().hdfs_dataset_dir_url
        hdfs_path = self.context._jvm.Path(  # type: ignore
            dataset_url + "/" + filename + "." + ext)
        return self._fs.exists(hdfs_path)

    def wait_for_file_on_hdfs(self, filename: str, format: DataFormat) -> None:
        # Wait for the dataset to be available on HDFS
        logger = LoggerFactory.spark()
        logger.log("Checking if dataset exists on HDFS..")
        logged = False
        while not self.file_exists_on_hdfs(filename, ext=format.name.lower()):
            if not logged:
                logger.log("Dataset not found on HDFS, waiting..")
                logged = True
            time.sleep(5)

        logger.log("Dataset found on HDFS, proceeding..")

    def read_parquet_from_hdfs(self, filename: str) -> DataFrame:
        config = ConfigFactory.config()
        return self._session.read.parquet(config.hdfs_dataset_dir_url + "/" + filename + ".parquet", header=True)

    def read_avro_from_hdfs(self, filename: str) -> DataFrame:
        config = ConfigFactory.config()
        return self._session.read.format("avro").load(config.hdfs_dataset_dir_url + "/" + filename + ".avro", header=True)

    def read_from_hdfs(self, data_format: DataFormat, filename: str) -> DataFrame:
        """Reads the dataset from HDFS in the specified format."""

        if data_format == DataFormat.PARQUET:
            return self.read_parquet_from_hdfs(filename)
        elif data_format == DataFormat.AVRO:
            return self.read_avro_from_hdfs(filename)
        else:
            raise ValueError("Invalid data format")

    def df_from_action_result(self, action_res: SparkActionResult) -> DataFrame:
        df = SparkAPI.get().session.createDataFrame(
            action_res.result, schema=action_res.header)

        if action_res.ascending_list is None:
            return df.sort(action_res.sort_list)
        else:
            return df.sort(action_res.sort_list, ascending=action_res.ascending_list)

    def read_preprocessed_data_and_persist(self, filename: str, format: DataFormat) -> tuple[RDD, DataFrame]:
        """Read preprocessed data from HDFS and persist it."""
        df = self.read_from_hdfs(format, filename)
        # Persist the DataFrame and RDD
        df = df.persist()
        rdd = df.rdd.persist()
        # Trigger an action to persist the data
        df.count()
        rdd.count()

        # Create a temporary view for Spark SQL queries
        df.createOrReplaceTempView("DisksMonitor")

        return (rdd, df)

    def write_to_hdfs(self, df: DataFrame, filename: str, format: DataFormat) -> None:
        """Write DataFrame to HDFS in the specified format."""
        df = df.coalesce(1)
        config = ConfigFactory.config()
        filename = filename + "." + format.name.lower()
        if format == DataFormat.PARQUET:
            df.write.parquet(config.hdfs_dataset_dir_url +
                             "/" + filename, mode="overwrite")
        elif format == DataFormat.AVRO:
            df.write.format("avro").save(
                config.hdfs_dataset_dir_url + "/" + filename, mode="overwrite", header=True)
        else:
            raise ValueError("Invalid data format")

    def write_results_to_hdfs(self, df: DataFrame, filename: str) -> None:
        """Write query result to HDFS."""
        df = df.coalesce(1)
        df.write.csv(
            ConfigFactory.config().hdfs_results_dir_url + "/" + filename,
            mode="overwrite",
            header=True,
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
