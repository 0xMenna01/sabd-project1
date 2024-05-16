from __future__ import annotations
from typing import Iterable, Optional, TypeVar


from pyspark import RDD, SparkContext
from pyspark.sql import SparkSession
from . import SparkBuilder


T = TypeVar("T")


class SparkAPI:
    _instance = None

    def __init__(self, session: SparkSession) -> None:
        self._session: SparkSession = session

    @staticmethod
    def get() -> SparkAPI:
        if SparkAPI._instance is None:
            SparkAPI._instance = SparkBuilder.from_default_config().build()
        return SparkAPI._instance

    @property
    def context(self) -> SparkContext:
        return self._session.sparkContext

    @property
    def session(self) -> SparkSession:
        return self._session

    def parallelize(self, c: Iterable[T], numSlices: Optional[int] = None) -> RDD[T]:
        return self._session.sparkContext.parallelize(c, numSlices)
