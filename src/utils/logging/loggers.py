import json
from typing import TypedDict
from loguru import logger


class Logger:
    def __init__(self):
        pass


class AppLogger(Logger):
    """Generic Logger"""

    def __init__(self):
        super().__init__()

    def config_loaded(self, hdfs_config, spark_config):
        logger.info(
            f"{json.dumps(hdfs_config)} and {json.dumps(spark_config)}")


class SparkLogger(Logger):
    """Logger for Spark"""

    def __init__(self):
        super().__init__()

    def spark_api_loaded(self):
        logger.info("Spark API loaded successfully..")


class B2Logger(Logger):
    """Logger for B2"""

    def __init__(self):
        super().__init__()

    def b2_api_loaded(self):
        logger.info("B2 Authentication SUCCESS, API loaded..")

    def b2_bucket_loaded(self):
        logger.info("Bucket Loaded successfully..")

    def downloading_file(self,):
        logger.info(f"Downloading file in memory..")

    def decrypting_file(self):
        logger.info("Decrypting file..")

    def file_decrypted(self):
        logger.info("File decrypted..")
