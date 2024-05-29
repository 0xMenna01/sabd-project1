import time
from pyspark.sql import DataFrame
from api.spark import SparkAPI
from utils.logging.factory import LoggerFactory
from spark.model import SparkActionResult, QueryResult


def exec_query(df: DataFrame) -> QueryResult:
    # @param df : DataFrame of ['event_date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']

    # SQL query
    sql_query = """
        SELECT event_date, vault_id, SUM(failure) AS  failures_count
        FROM DisksMonitor
        WHERE failure > 0
        GROUP BY event_date, vault_id
        HAVING failures_count IN (2, 3, 4)
    """

    result_df = SparkAPI.get().session.sql(sql_query)

    logger = LoggerFactory.spark()
    logger.log("Starting to evaluate action of SQL query 1..")
    start_time = time.time()
    # Triggers an action
    res = result_df.collect()
    end_time = time.time()

    res = QueryResult(name="sql-query1-evaluation", results=[SparkActionResult(
        name="sql-query1",
        header=["event_date", "vault_id", "failures_count"],
        sort_list=["event_date", "vault_id", "failures_count"],
        result=res,
        execution_time=end_time - start_time
    )])
    logger.log(
        f"SQL query 1 took {res.total_exec_time} seconds..")

    return res
