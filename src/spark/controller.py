from model import QueryFramework, QueryNum, DataFormat
from api.spark import SparkAPI
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType


class SparkController:
    """Controller for executing queries and writing results using Spark."""

    def __init__(self, framework: QueryFramework, query_num: QueryNum, write_results: bool = True, write_eval: bool = False):
        self._framework = framework
        self._query_num = query_num
        self._write_results = write_results
        self._write_eval = write_eval
        # Data structures for processing and storing results
        self._data_format = None
        self._rdd = None
        self._data_frame = None
        self._rdd_results = None
        self._data_frame_results = None
        self._current_evaluation = None

    def set_data_format(self, data_format: DataFormat) -> None:
        """Set the format of the data to read."""
        self._data_format = data_format

    def prepare_for_processing(self) -> None:
        assert self._data_format is not None, "Data format not set"

        # Retrieve the dataframe by reading from HDFS based on the data format
        df = SparkAPI.get().read_from_hdfs(self._data_format)

        # Delete rows with missing values and duplicates
        df = df.dropna().drop_duplicates()

        df = (
            df
            # Extarct only the date
            .withColumn('date', to_date(
                col('date'), 'yyyy-MM-dd'))
            # Filter out invalid serial numbers
            .withColumn('serial_number', col('serial_number').rlike('^[A-Z0-9]{8,}$'))
            # Filter out invalid models
            .withColumn('model', col('model').rlike('^[A-Z0-9]+$'))
            # Cast the remaining columns to the correct types
            .withColumn('failure', col('failure').cast('int'))
            .withColumn('vault_id', col('vault_id').cast('int'))
            .withColumn('power_on_hours', col('power_on_hours').cast(DoubleType()))
        )

        # Persist both data frame and RDD (happens lazily)
        df = df.persist()
        rdd = df.rdd.persist()
        # We need to trigger an action to actually persist the data
        df.count()
        rdd.count()

        df.createOrReplaceTempView("DisksMonitor")
        # Rename date column because sql queries use date as a keyword
        df = df.withColumnRenamed("date", "event_date")

        # Store the preprocecessed data
        self._data_frame = df
        self._rdd = rdd

    def process_data(self) -> None:
        """Process the data using the specified framework and query."""

        # Ensure data is prepared for processing
        assert self._data_frame is not None and self._rdd is not None, "Data not prepared for processing"

        if self._framework == QueryFramework.SPARK_CORE:
            if self._query_num == QueryNum.QUERY_ALL:
                # Execute all queries with Spark Core
                for query in QueryNum:
                    query_spark_core(query, self._rdd)
            else:
                # Execute a single query with Spark Core
                query_spark_core(self._query_num, self._rdd)

        elif self._framework == QueryFramework.SPARK_SQL:
            if self._query_num == QueryNum.QUERY_ALL:
                # Execute all queries with Spark SQL
                for query in QueryNum:
                    query_spark_sql(query, self._data_frame)
            else:
                # Execute a single query with Spark SQL
                query_spark_sql(self._query_num, self._data_frame)

        elif self._framework == QueryFramework.SPARK_CORE_AND_SQL:
            if self._query_num == QueryNum.QUERY_ALL:
                # Execute all queries with Spark Core and Spark SQL
                for query in QueryNum:
                    query_spark_core(query, self._rdd)
                    query_spark_sql(query, self._data_frame)
            else:
                # Execute a single query with Spark Core and Spark SQL
                query_spark_core(self._query_num, self._rdd)
                query_spark_sql(self._query_num, self._data_frame)


def query_spark_core(query_num: QueryNum, rdd: RDD[Row]) -> None:
    """Executes a query using Spark Core."""
    if query_num == QueryNum.QUERY_ONE:
        # Query 1
        pass
    elif query_num == QueryNum.QUERY_TWO:
        # Query 2
        pass
    elif query_num == QueryNum.QUERY_THREE:
        # Query 3
        pass


def query_spark_sql(query_num: QueryNum, data_frame: DataFrame) -> None:
    """Executes a query using Spark SQL."""
    if query_num == QueryNum.QUERY_ONE:
        # Query 1
        pass
    elif query_num == QueryNum.QUERY_TWO:
        # Query 2
        pass
    elif query_num == QueryNum.QUERY_THREE:
        # Query 3
        pass


def is_valid_serial_number(serial_number: str) -> bool:
    """
    Validates the serial number field.
    :param serial_number: str
    :return: bool
    """
    # Check if the serial number contains only uppercase alphanumeric characters
    return serial_number.isalnum() and serial_number.isupper()
