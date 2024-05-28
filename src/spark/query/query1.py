import time
from pyspark.rdd import RDD
from pyspark.sql import Row, DataFrame, functions as F
from utils.logging.factory import LoggerFactory


def exec_query(rdd: RDD[Row]) -> tuple[RDD, float]:
    # @param rdd : RDD of ['event_date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']

    # Process the RDD
    res_rdd = (
        rdd
        # Early filter to reduce data size
        .filter(lambda x: x.failure > 0)
        # Convert to ((date, vault_id), failure)
        .map(lambda x: ((x.event_date, x.vault_id), x.failure))
        # Sum failures per (date, vault_id)
        .reduceByKey(lambda acc, failure: acc + failure)
        # Filter based on lookup failures
        .filter(lambda x: x[1] in [2, 3, 4])
    )

    logger = LoggerFactory.spark()
    logger.log("Starting to evaluate query 1..")
    start_time = time.time()
    # Triggers an action
    res_rdd.collect()
    end_time = time.time()
    logger.log(
        f"Query 1 took {end_time - start_time} seconds..")

    return (res_rdd, end_time - start_time)
