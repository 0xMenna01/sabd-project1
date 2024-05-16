import json
from loguru import logger


def config_loaded(hdfs_config, spark_config):
    logger.info(
        f"{json.dumps(hdfs_config)} and {json.dumps(spark_config)}")
