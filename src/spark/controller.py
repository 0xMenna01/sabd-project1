from __future__ import annotations

from model import QueryFramework, QueryNum, DataFormat, SparkError
from api.spark import SparkAPI
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType
# Spark core queries
import query.query1
import query.query2
import query.query3
# Spark SQL queries
import query_sql.query_sql1
import query_sql.query_sql2
import query_sql.query_sql3


class SparkController:
    """Controller for executing queries and writing results using Spark."""

    def __init__(self, framework: QueryFramework, query_num: QueryNum):
        self._framework = framework
        self._query_num = query_num
        # Data structures for processing and storing results
        self._data_format = None
        self._rdd = None
        self._data_frame = None
        self._rdd_results = []
        self._data_frame_results = []
        self._core_evaluations = []
        self._sql_evaluations = []

    def set_data_format(self, data_format: DataFormat) -> SparkController:
        """Set the format of the data to read."""
        self._data_format = data_format

        return self

    def prepare_for_processing(self) -> SparkController:
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

        return self

    def process_data(self) -> SparkController:
        """Process the data using the specified framework and query."""

        # Ensure data is prepared for processing
        assert self._data_frame is not None and self._rdd is not None, "Data not prepared for processing"

        # Query with Spark Core
        if self._framework == QueryFramework.SPARK_CORE:
            if self._query_num == QueryNum.QUERY_ALL:
                # Execute all queries with Spark Core
                for query in QueryNum:
                    (res_rdd, eval_time) = query_spark_core(query, self._rdd)
                    self._rdd_results.append(res_rdd)
                    self._core_evaluations.append(eval_time)
            else:
                # Execute a single query with Spark Core
                (res_rdd, eval_time) = query_spark_core(
                    self._query_num, self._rdd)
                self._rdd_results = [res_rdd]
                self._core_evaluations = [eval_time]

        # Query with Spark SQL
        elif self._framework == QueryFramework.SPARK_SQL:
            if self._query_num == QueryNum.QUERY_ALL:
                # Execute all queries with Spark SQL
                for query in QueryNum:
                    (res_df, eval_time) = query_spark_sql(
                        query, self._data_frame)
                    self._data_frame_results.append(res_df)
                    self._sql_evaluations.append(eval_time)

            else:
                # Execute a single query with Spark SQL
                (res_df, eval_time) = query_spark_sql(
                    self._query_num, self._data_frame)
                self._data_frame_results = [res_df]
                self._sql_evaluations = [eval_time]

        # Query with both Spark Core and Spark SQL
        elif self._framework == QueryFramework.SPARK_CORE_AND_SQL:
            if self._query_num == QueryNum.QUERY_ALL:
                # Execute all queries with Spark Core and Spark SQL
                for query in QueryNum:
                    (res_rdd, eval_core_time) = query_spark_core(query, self._rdd)
                    (res_df, eval_sql_time) = query_spark_sql(
                        query, self._data_frame)
                    self._rdd_results.append(res_rdd)
                    self._data_frame_results.append(res_df)
                    self._core_evaluations.append(eval_core_time)
                    self._sql_evaluations.append(eval_sql_time)
            else:
                # Execute a single query with Spark Core and Spark SQL
                (res_rdd, eval_core_time) = query_spark_core(
                    self._query_num, self._rdd)
                (res_df, eval_sql_time) = query_spark_sql(
                    self._query_num, self._data_frame)
                self._rdd_results = [res_rdd]
                self._data_frame_results = [res_df]
                self._core_evaluations = [eval_core_time]
                self._sql_evaluations = [eval_sql_time]

        return self


def query_spark_core(query_num: QueryNum, rdd: RDD[Row]) -> tuple[RDD, float]:
    """Executes a query using Spark Core."""
    if query_num == QueryNum.QUERY_ONE:
        # Query 1
        return query.query1.exec_query(rdd)
    elif query_num == QueryNum.QUERY_TWO:
        # Query 2
        return query.query1.exec_query(rdd)
    elif query_num == QueryNum.QUERY_THREE:
        return query.query1.exec_query(rdd)
    else:
        raise SparkError("Invalid query")


def query_spark_sql(query_num: QueryNum, data_frame: DataFrame) -> tuple[DataFrame, float]:
    """Executes a query using Spark SQL."""
    if query_num == QueryNum.QUERY_ONE:
        # Query 1
        return query_sql.query_sql1.exec_query(data_frame)
    elif query_num == QueryNum.QUERY_TWO:
        # Query 2
        return query_sql.query_sql1.exec_query(data_frame)
    elif query_num == QueryNum.QUERY_THREE:
        # Query 3
        return query_sql.query_sql1.exec_query(data_frame)
    else:
        raise SparkError("Invalid SQL query")
