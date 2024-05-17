from typing import Optional
from .parser import Config
from utils.logging.factory import LoggerFactory


class ConfigFactory:
    _config_instance: Optional[Config] = None

    @staticmethod
    def config() -> Config:
        if ConfigFactory._config_instance is None:
            ConfigFactory._config_instance = Config.from_default_config()
            LoggerFactory.app_logger().config_loaded(
                ConfigFactory._config_instance._hdfs, ConfigFactory._config_instance._spark)
        return ConfigFactory._config_instance
