import time
from pyspark.sql import DataFrame
from api.spark import SparkAPI
from utils.logging.factory import LoggerFactory


def exec_query(df: DataFrame) -> tuple[DataFrame, float]:
    # @param df : DataFrame of ['event_date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']

    # SQL query
    sql_query = """
        SELECT event_date, vault_id, SUM(failure) AS total_failures
        FROM DisksMonitor
        WHERE failure > 0
        GROUP BY date, vault_id
        HAVING total_failures IN (2, 3, 4)
    """

    result_df = SparkAPI.get().session.sql(sql_query)

    logger = LoggerFactory.spark()
    logger.log("Starting to evaluate SQL query 1")
    start_time = time.time()
    # Triggers an action
    result_df.collect()
    end_time = time.time()
    logger.log(
        f"SQL query 1 took {end_time - start_time} seconds..")

    return (result_df, end_time - start_time)
