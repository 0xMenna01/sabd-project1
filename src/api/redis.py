from __future__ import annotations

import json
from redis import Redis
from utils.config.factory import ConfigFactory
from pyspark.sql import DataFrame
from api.spark import SparkAPI


class RedisAPI:
    _instance = None

    def __init__(self, redis_connection: Redis) -> None:
        self._redis_connection = redis_connection

    @staticmethod
    def get() -> RedisAPI:
        if RedisAPI._instance is None:
            config = ConfigFactory.config()
            RedisAPI._instance = RedisAPI(
                Redis(host=config.redis_host, port=config.redis_port, db=config.redis_db))
        return RedisAPI._instance

    def put_result(self, query: str, df: DataFrame):

        json_res = df.toJSON().collect()
        json_arr = json.dumps(json_res)

        self._redis_connection.set(query, json_arr)
