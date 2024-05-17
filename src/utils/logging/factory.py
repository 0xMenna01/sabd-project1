from utils.logging.loggers import Logger, AppLogger, SparkLogger, B2Logger


class LoggerFactory:
    _app_logger = None
    _spark_logger = None
    _b2_logger = None

    @staticmethod
    # create different methods for each logger type
    def app_logger() -> AppLogger:
        if LoggerFactory._app_logger is None:
            LoggerFactory._app_logger = AppLogger()
        return LoggerFactory._app_logger

    @staticmethod
    def spark_logger() -> SparkLogger:
        if LoggerFactory._spark_logger is None:
            LoggerFactory._spark_logger = SparkLogger()
        return LoggerFactory._spark_logger

    @staticmethod
    def b2_logger() -> B2Logger:
        if LoggerFactory._b2_logger is None:
            LoggerFactory._b2_logger = B2Logger()
        return LoggerFactory._b2_logger
