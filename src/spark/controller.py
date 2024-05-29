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


class SparkController:
    """Controller for executing queries and writing results using Spark."""

    def __init__(self, framework: QueryFramework, query_num: QueryNum, local_write: bool = False, write_evaluation: bool = False):
        self._framework = framework
        self._query_num = query_num
        self._local_write = local_write
        self._write_evaluation = write_evaluation
        # Data structures for processing and storing results
        self._data_format: DataFormat | None = None
        self._rdd: RDD | None = None
        self._data_frame: DataFrame | None = None
        self._results: list[QueryResult] = []

    def set_data_format(self, data_format: DataFormat) -> SparkController:
        """Set the format of the data to read."""
        self._data_format = data_format
        
        # Wait for the dataset to be available on HDFS
        api = SparkAPI.get()
        logger = LoggerFactory.spark()
        logger.log("Checking if dataset exists on HDFS..")
        logged = False
        while not api.dataset_exists_on_hdfs(ext=data_format.name.lower()):
            if not logged:
                logger.log("Dataset not found on HDFS, waiting..")
                logged = True
            time.sleep(5)
        
        logger.log("Dataset found on HDFS, proceeding..")
        return self

    def prepare_for_processing(self) -> SparkController:

        assert self._data_format is not None, "Data format not set"

        LoggerFactory.spark().log("Reading data from HDFS in format: " + self._data_format.name)
        # Retrieve the dataframe by reading from HDFS based on the data format
        df = SparkAPI.get().read_from_hdfs(self._data_format)
        
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
        df = df.persist()
        rdd = df.rdd.map(tuple)
        rdd = rdd.persist()
        # Trigger an action to persist the data
        df.count()
        rdd.count()
        
        # Create a temporary view for Spark SQL queries
        df.createOrReplaceTempView("DisksMonitor")
        
        # Store the preprocecessed data
        self._data_frame = df
        self._rdd = rdd

        LoggerFactory.spark().log("Data prepared for processing.")

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
                    res = query_spark_core(query, self._data_frame)
                    self._results.append(res)

            else:
                # Execute a single query with Spark Core
                res = query_spark_core(
                    self._query_num, self._data_frame)
                self._results.append(res)

        # Query with Spark SQL
        elif self._framework == QueryFramework.SPARK_SQL:
            if self._query_num == QueryNum.QUERY_ALL:
                # Execute all queries with Spark SQL
                for query in QueryNum:
                    res = query_spark_sql(
                        query, self._data_frame)
                    self._results.append(res)

            else:
                # Execute a single query with Spark SQL
                res = query_spark_sql(
                    self._query_num, self._data_frame)
                self._results.append(res)

        # Query with both Spark Core and Spark SQL
        elif self._framework == QueryFramework.SPARK_CORE_AND_SQL:
            if self._query_num == QueryNum.QUERY_ALL:
                # Execute all queries with Spark Core and Spark SQL
                for query in QueryNum:
                    res = query_spark_core(query, self._data_frame)
                    self._results.append(res)
                    res = query_spark_sql(
                        query, self._data_frame)
                    self._results.append(res)

            else:
                # Execute a single query with Spark Core and Spark SQL
                res = query_spark_core(
                    self._query_num, self._data_frame)
                self._results.append(res)
                res = query_spark_sql(
                    self._query_num, self._data_frame)
                self._results.append(res)

        return self

    def write_results(self) -> None:
        """Write the results."""
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
                config = ConfigFactory.config()
                LoggerFactory.spark().log("Writing results to HDFS..")
                files.write_to_hdfs_as_csv(df, filename)

            if self._write_evaluation:
                LoggerFactory.spark().log("Writing evaluation..")
                files.write_evaluation(res.name, res.total_exec_time)


def query_spark_core(query_num: QueryNum, df: DataFrame) -> QueryResult:
    """Executes a query using Spark Core."""
    if query_num == QueryNum.QUERY_ONE:
        LoggerFactory.spark().log("Executing query 1 with Spark Core..")
        # Query 1
        return query1.exec_query(df)
    elif query_num == QueryNum.QUERY_TWO:
        LoggerFactory.spark().log("Executing query 2 with Spark Core..")
        # Query 2
        return query2.exec_query(df)
    elif query_num == QueryNum.QUERY_THREE:
        LoggerFactory.spark().log("Executing query 3 with Spark Core..")
        return query1.exec_query(df)
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
        return query_sql1.exec_query(data_frame)
    elif query_num == QueryNum.QUERY_THREE:
        LoggerFactory.spark().log("Executing query 3 with Spark SQL..")
        # Query 3
        return query_sql1.exec_query(data_frame)
    else:
        raise SparkError("Invalid SQL query")
