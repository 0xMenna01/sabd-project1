from __future__ import annotations

import json
import os
from typing import Optional, TypedDict
import pandas as pd
from . import logger


DEFAULT_CONFIG_PATH = os.getenv(
    'CONFIG_PATH', 'config/default-config.json')


class HdfsConfig(TypedDict):
    host: str
    port: int


class SparkConfig(TypedDict):
    master: str
    appName: str
    port: int


class Config:
    """Configuration of the application."""

    def __init__(self, hdfs: HdfsConfig, spark: SparkConfig) -> None:
        self._hdfs = hdfs
        self._spark = spark

    @staticmethod
    def from_default_config() -> Config:
        """Utility to load the default configuration from a JSON file."""
        with open(DEFAULT_CONFIG_PATH, 'r') as file:
            config_data = json.load(file)

        return Config(hdfs=config_data['hdfs'], spark=config_data['spark'])

    @property
    def hdfs_host(self) -> str:
        return self._hdfs['host']

    @property
    def hdfs_port(self) -> int:
        return self._hdfs['port']

    @property
    def spark_master(self) -> str:
        return self._spark['master']

    @property
    def spark_app_name(self) -> str:
        return self._spark['appName']

    @property
    def spark_port(self) -> int:
        return self._spark['port']


class ConfigFactory:
    _config_instance: Optional[Config] = None

    @staticmethod
    def config() -> Config:
        if ConfigFactory._config_instance is None:
            ConfigFactory._config_instance = Config.from_default_config()
            logger.config_loaded(
                ConfigFactory._config_instance._hdfs, ConfigFactory._config_instance._spark)
        return ConfigFactory._config_instance
