from __future__ import annotations
import os
import time

from .model import QueryFramework, QueryNum, DataFormat, SparkError, QueryResult
from api.spark import SparkAPI
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, DateType, StringType
# Spark core queries
from .query import query1, query2, query3
# Spark SQL queries
from .query_sql import query_sql1, query_sql2, query_sql3
from utils.config.factory import ConfigFactory
from utils import files
from utils.logging.factory import LoggerFactory

DATASET_FILE_NAME = "dataset"
PRE_PROCESSED_FILE_NAME = "preprocessed"


class SparkController:
    """Controller for executing queries and writing results using Spark."""

    def __init__(self, framework: QueryFramework, query_num: QueryNum, local_write: bool = False, write_evaluation: bool = False):
        self._framework = framework
        self._query_num = query_num
        self._local_write = local_write
        self._write_evaluation = write_evaluation
        self._data_format = None
        # For storing the results
        self._results: list[QueryResult] = []

    def set_data_format(self, data_format: DataFormat) -> SparkController:
        """Set the format of the data to read."""
        self._data_format = data_format

        return self

    def prepare_for_processing(self) -> SparkController:
        """Preprocess data and store the result on HDFS for checkpointing and later processing."""
        assert self._data_format is not None, "Data format not set"
        api = SparkAPI.get()

        # Wait for the dataset to be available on HDFS
        api.wait_for_file_on_hdfs(DATASET_FILE_NAME, self._data_format)

        LoggerFactory.spark().log("Reading data from HDFS in format: " + self._data_format.name)
        # Retrieve the dataframe by reading from HDFS based on the data format
        df = SparkAPI.get().read_from_hdfs(self._data_format, DATASET_FILE_NAME)

        LoggerFactory.spark().log("Preparing data for processing..")
        # Delete rows with missing values and duplicates
        df = df.dropna().dropDuplicates()
        df = df.select(
            df.date.cast(DateType()),
            df.serial_number.cast(StringType()),
            df.model.cast(StringType()),
            df.failure.cast(IntegerType()),
            df.vault_id.cast(IntegerType()),
            df.s9_power_on_hours.cast(DoubleType())
        )
        # Filter out invalid rows
        df = (
            df
            .where(df.date.isNotNull())
            .where(col('serial_number').rlike('^[A-Z0-9]{8,}$'))
            .where(col('model').rlike('^[A-Z0-9]+$'))
            .where(col('failure').isNotNull())
            .where(col('vault_id').isNotNull())
            .where(col('s9_power_on_hours').isNotNull())
        )

        df = df.withColumnRenamed("date", "event_date")

        LoggerFactory.spark().log(
            "Data prepared for processing, storing it on HDFS for checkpointing and later usage.")
        api.write_to_hdfs(
            df, filename=PRE_PROCESSED_FILE_NAME, format=self._data_format)

        return self

    def process_data(self) -> SparkController:
        """Process the data using the specified framework and query."""
        assert self._data_format is not None, "Data format not set"

        # Ensure data is prepared for processing
        api = SparkAPI.get()
        assert api.file_exists_on_hdfs(PRE_PROCESSED_FILE_NAME, self._data_format.name.lower(
        )), "Data not prepared for processing"

        LoggerFactory.spark().log("Reading preprocessed data from HDFS..")
        (rdd, df) = api.read_preprocessed_data_and_persist(
            PRE_PROCESSED_FILE_NAME, self._data_format)

        # Query with Spark Core
        if self._framework == QueryFramework.SPARK_CORE:
            if self._query_num == QueryNum.QUERY_ALL:
                # Execute all queries with Spark Core
                for query in QueryNum:
                    res = query_spark_core(query, rdd)
                    self._results.append(res)

            else:
                # Execute a single query with Spark Core
                res = query_spark_core(self._query_num, rdd)
                self._results.append(res)

        # Query with Spark SQL
        elif self._framework == QueryFramework.SPARK_SQL:
            if self._query_num == QueryNum.QUERY_ALL:
                # Execute all queries with Spark SQL
                for query in QueryNum:
                    res = query_spark_sql(
                        query, df)
                    self._results.append(res)

            else:
                # Execute a single query with Spark SQL
                res = query_spark_sql(
                    self._query_num, df)
                self._results.append(res)

        # Query with both Spark Core and Spark SQL
        elif self._framework == QueryFramework.SPARK_CORE_AND_SQL:
            if self._query_num == QueryNum.QUERY_ALL:
                # Execute all queries with Spark Core and Spark SQL
                for query in QueryNum:
                    res = query_spark_core(query, rdd)
                    self._results.append(res)
                    res = query_spark_sql(
                        query, df)
                    self._results.append(res)

            else:
                # Execute a single query with Spark Core and Spark SQL
                res = query_spark_core(self._query_num, rdd)
                self._results.append(res)
                res = query_spark_sql(
                    self._query_num, df)
                self._results.append(res)

        return self

    def write_results(self) -> None:
        """Write the results."""
        assert self._data_format is not None, "Data format not set"
        assert self._results, "No results to write"

        api = SparkAPI.get()
        for res in self._results:
            # This is executed maximum twice
            for output_res in res:
                filename = output_res.name + ".csv"
                df = api.df_from_action_result(output_res)
                if self._local_write:
                    out_path = files.results_path_from_filename(filename)
                    LoggerFactory.spark().log("Writing results to: " + out_path)

                    files.write_result_as_csv(
                        res_df=df, out_path=out_path)

                # Write results to HDFS
                LoggerFactory.spark().log("Writing results to HDFS..")
                api.write_results_to_hdfs(df, filename)

            if self._write_evaluation:
                LoggerFactory.spark().log("Writing evaluation..")
                files.write_evaluation(
                    res.name, self._data_format.name.lower(), res.total_exec_time)


def query_spark_core(query_num: QueryNum, rdd: RDD) -> QueryResult:
    """Executes a query using Spark Core. Using both RDD and DataFrame."""
    if query_num == QueryNum.QUERY_ONE:
        LoggerFactory.spark().log("Executing query 1 with Spark Core..")
        # Query 1
        return query1.exec_query(rdd)
    elif query_num == QueryNum.QUERY_TWO:
        LoggerFactory.spark().log("Executing query 2 with Spark Core..")
        # Query 2
        return query2.exec_query(rdd)
    elif query_num == QueryNum.QUERY_THREE:
        LoggerFactory.spark().log("Executing query 3 with Spark Core..")
        return query3.exec_query(rdd)
    else:
        raise SparkError("Invalid query")


def query_spark_sql(query_num: QueryNum, data_frame: DataFrame) -> QueryResult:
    """Executes a query using Spark SQL."""
    if query_num == QueryNum.QUERY_ONE:
        LoggerFactory.spark().log("Executing query 1 with Spark SQL..")
        # Query 1
        return query_sql1.exec_query(data_frame)
    elif query_num == QueryNum.QUERY_TWO:
        LoggerFactory.spark().log("Executing query 2 with Spark SQL..")
        # Query 2
        return query_sql2.exec_query(data_frame)
    elif query_num == QueryNum.QUERY_THREE:
        LoggerFactory.spark().log("Executing query 3 with Spark SQL..")
        # Query 3
        return query_sql3.exec_query(data_frame)
    else:
        raise SparkError("Invalid SQL query")
