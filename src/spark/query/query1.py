import time
from pyspark.rdd import RDD
from pyspark.sql import Row, DataFrame, functions as F
from utils.logging.factory import LoggerFactory
from spark.model import SparkActionResult, QueryResult
from api.spark import SparkAPI


def exec_query(rdd: RDD) -> QueryResult:
    # @param rdd : RDD of ['event_date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']

    # Process the RDD
    res_rdd = (
        rdd
        # Early filter to reduce data size
        .filter(lambda x: x[3] > 0)
        # Convert to ((event_date, vault_id), failure)
        .map(lambda x: ((x[0], x[4]), x[3]))
        # Sum failures per (date, vault_id)
        .reduceByKey(lambda acc, failure: acc + failure)
        # Filter based on lookup failures
        .filter(lambda x: x[1] in [2, 3, 4])
        # Convert to (event_date, vault_id, failures_count)
        .map(lambda x: (x[0][0], x[0][1], x[1]))
    )

    logger = LoggerFactory.spark()
    logger.log("Starting to evaluate the spark action of query 1..")
    start_time = time.time()
    # Triggers the action
    out_res = res_rdd.collect()
    end_time = time.time()
    logger.log("Finished evaluating..")

    res = QueryResult(name="query1-evalutaion", results=[SparkActionResult(
        name="query1",
        header=["event_date", "vault_id", "failures_count"],
        sort_list=["event_date", "vault_id", "failures_count"],
        result=out_res,
        execution_time=end_time - start_time
    )])
    logger.log(
        f"Query 1 took {res.total_exec_time} seconds..")

    return res
