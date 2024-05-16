from __future__ import annotations

from pyspark import SparkContext
from pyspark.sql import SparkSession
from utils.config_parser import ConfigFactory
from . import SparkAPI


class SparkBuilder:
    """Builds the Spark API."""

    def __init__(self, master: str, app_name: str, port: int):
        self._master_url = "spark://" + master + ":" + port
        self._app_name = app_name

    @staticmethod
    def from_default_config() -> SparkBuilder:
        config = ConfigFactory.config()
        return SparkBuilder(config.spark_master, config.spark_app_name, config.spark_port)

    def build(self) -> SparkAPI:
        context = SparkContext(self._master_url, self._app_name)
        return SparkAPI(SparkSession(sparkContext=context))
