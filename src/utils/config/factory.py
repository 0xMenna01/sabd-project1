from typing import Optional
from .parser import Config
from utils.logging.factory import LoggerFactory


class ConfigFactory:
    _config_instance: Optional[Config] = None

    @staticmethod
    def config() -> Config:
        if ConfigFactory._config_instance is None:
            # Load the default configuration
            ConfigFactory._config_instance = Config.from_default_config()
            # Log the configuration
            LoggerFactory.app().config_loaded(
                ConfigFactory._config_instance._hdfs, ConfigFactory._config_instance._spark, ConfigFactory._config_instance._b2, ConfigFactory._config_instance._nifi)
            
        return ConfigFactory._config_instance
