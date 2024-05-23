from __future__ import annotations

import json
import os
from typing import TypedDict
import pandas as pd


DEFAULT_CONFIG_PATH = os.getenv(
    'CONFIG_PATH', 'config/default-config.json')


class HdfsConfig(TypedDict):
    host: str
    port: int
    path: str


class SparkConfig(TypedDict):
    master: str
    appName: str
    port: int


class B2Config(TypedDict):
    bucketName: str
    fileName: str


class NifiConfig(TypedDict):
    host: str
    port: int


class Config:
    """Configuration of the application."""

    def __init__(self, hdfs: HdfsConfig, spark: SparkConfig, b2: B2Config, nifi: NifiConfig) -> None:
        self._hdfs = hdfs
        self._spark = spark
        self._b2 = b2
        self._nifi = nifi

    @staticmethod
    def from_default_config() -> Config:
        """Utility to load the default configuration from a JSON file."""
        with open(DEFAULT_CONFIG_PATH, 'r') as file:
            config_data = json.load(file)

        return Config(hdfs=config_data['hdfs'], spark=config_data['spark'], b2=config_data['b2'], nifi=config_data['nifi'])

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

    @property
    def b2_bucket_name(self) -> str:
        return self._b2['bucketName']

    @property
    def b2_file_name(self) -> str:
        return self._b2['fileName']

    @property
    def nifi_endpoint(self) -> str:
        return "https://" + self._nifi['host'] + ":" + str(self._nifi['port'])

    @property
    def hdfs_dataset_path(self) -> str:
        return "hdfs://" + self.hdfs_host + ":" + str(self.hdfs_port) + self._hdfs['path']
