from pyspark.context import SparkContext


class SparkLogger:
    def __init__(self):
        pass

    ERROR = 1
    WARNING = 2
    INFO = 3
    DEBUG = 4

    @staticmethod
    def _log(msg, level):
        sc = SparkContext.getOrCreate()
        log4jLogger = sc._jvm.org.apache.log4j
        LOGGER = log4jLogger.LogManager.getLogger(__name__)
        if LOGGER:
            if level == 1:
                LOGGER.error(msg)
            elif level == 2:
                LOGGER.warn(msg)
            elif level == 3:
                LOGGER.info(msg)
            else:
                LOGGER.debug(msg)
        else:
            print("Logging level {} : {}".format(level, msg))

    @staticmethod
    def error(msg):
        SparkLogger._log(msg, level=SparkLogger.ERROR)

    @staticmethod
    def warn(msg):
        SparkLogger._log(msg, level=SparkLogger.WARNING)

    @staticmethod
    def info(msg):
        SparkLogger._log(msg, level=SparkLogger.INFO)

    @staticmethod
    def debug(msg):
        SparkLogger._log(msg, level=SparkLogger.DEBUG)

