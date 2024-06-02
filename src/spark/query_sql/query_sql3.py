import time
from pyspark.sql import DataFrame
from api.spark import SparkAPI
from utils.logging.factory import LoggerFactory
from spark.model import SparkActionResult, QueryResult


def exec_query(df: DataFrame) -> QueryResult:
    # @param df : DataFrame of ['event_date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']

    # SQL query
    sql_query = """
            WITH latest_s9 AS (
            SELECT
                serial_number,
                failure,
                s9_power_on_hours,
                ROW_NUMBER() OVER (PARTITION BY serial_number, failure ORDER BY date DESC) as rn
            FROM a
            )
            SELECT
                failure,
                MIN(s9_power_on_hours) as min,
                MAX(s9_power_on_hours) as max,
                COUNT(*) as count,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY s9_power_on_hours) as perc_25,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s9_power_on_hours) as perc_50,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY s9_power_on_hours) as perc_75
            FROM latest_s9
            WHERE rn = 1
            GROUP BY failure;
    """

    result_df = SparkAPI.get().session.sql(sql_query)

    logger = LoggerFactory.spark()
    logger.log("Starting to evaluate action of SQL query 3..")
    start_time = time.time()
    # Triggers an action
    res = result_df.collect()
    end_time = time.time()

    res = QueryResult(name="sql-query3", results=[SparkActionResult(
        name="sql-query3",
        header=["failure", "min", "max", "25_percentile",
                "50_percentile", "75_percentile", "count"],
        sort_list=["failure", "min", "max", "25_percentile",
                   "50_percentile", "75_percentile", "count"],
        result=res,
        execution_time=end_time - start_time
    )])
    logger.log(
        f"SQL query 3 took {res.total_exec_time} seconds..")

    return res
