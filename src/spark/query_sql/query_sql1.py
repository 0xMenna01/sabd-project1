import time
from pyspark.sql import DataFrame
from api.spark import SparkAPI
from utils.logging.factory import LoggerFactory
from spark.model import SparkActionResult, QueryResult
from spark.query.query1 import HEADER, SORT_LIST


def exec_query(df: DataFrame) -> QueryResult:
    # @param df : DataFrame of ['event_date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']

    # SQL query
    result_df = SparkAPI.get().session.sql("""
        SELECT event_date, vault_id, COUNT(failure) AS  failures_count
        FROM DisksMonitor
        WHERE failure = 1
        GROUP BY event_date, vault_id
        HAVING failures_count IN (2, 3, 4)
    """)

    logger = LoggerFactory.spark()
    logger.log("Starting to evaluate action of SQL query 1..")
    start_time = time.time()
    # Triggers an action
    res = result_df.collect()
    end_time = time.time()
    logger.log("Finished evaluating..")

    res = QueryResult(name="sql-query1", results=[SparkActionResult(
        name="sql-query1",
        header=HEADER,
        sort_list=SORT_LIST,
        result=res,
        execution_time=end_time - start_time
    )])
    logger.log(
        f"SQL query 1 took {res.total_exec_time} seconds..")

    return res
