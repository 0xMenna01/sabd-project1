import time
from pyspark.sql import DataFrame
from api.spark import SparkAPI
from utils.logging.factory import LoggerFactory
from spark.model import SparkActionResult, QueryResult
from spark.query.query3 import HEADER, SORT_LIST


def exec_query(df: DataFrame) -> QueryResult:
    # @param df : DataFrame of ['event_date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']
    api = SparkAPI.get()

    result_df = api.session.sql("""
        SELECT
            CAST(failure as INT) AS failure,
            MIN(s9_power_on_hours) AS min_power_on_hours,
            percentile_approx(s9_power_on_hours, 0.25) AS percentile_25th,
            percentile_approx(s9_power_on_hours, 0.5) AS percentile_50th,
            percentile_approx(s9_power_on_hours, 0.75) AS percentile_75th,
            MAX(s9_power_on_hours) AS max_power_on_hours,
            COUNT(*) AS count
        FROM
            (SELECT
                serial_number,
                MAX(s9_power_on_hours) AS s9_power_on_hours,
                MAX(failure) AS failure
            FROM
                DisksMonitor
            GROUP BY
                serial_number) AS LatestS9
        GROUP BY
            failure
    """)

    logger = LoggerFactory.spark()
    logger.log("Starting to evaluate action of SQL query 3..")
    start_time = time.time()
    # Triggers an action
    res = result_df.collect()
    end_time = time.time()

    res = QueryResult(name="sql-query3", results=[SparkActionResult(
        name="sql-query3",
        header=HEADER,
        sort_list=SORT_LIST,
        result=res,
        execution_time=end_time - start_time
    )])
    logger.log(
        f"SQL query 3 took {res.total_exec_time} seconds..")

    return res
