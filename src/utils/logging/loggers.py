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

    def config_loaded(self, hdfs_config, spark_config, b2_config, nifi_config):
        logger.info(
            f"{json.dumps(hdfs_config)}, {json.dumps(spark_config)}, {json.dumps(b2_config)}, {json.dumps(nifi_config)} loaded successfully..")


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


class NifiLogger(Logger):
    """Logger for Nifi"""

    def __init__(self):
        super().__init__()

    def nifi_connection_success(self):
        logger.info("Nifi connection has ben estabilished successfully..")

    def nifi_login_success(self):
        logger.info("Nifi login successful..")
