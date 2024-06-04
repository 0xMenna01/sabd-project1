import time
from numpy import double
from pyspark.rdd import RDD
from pyspark.sql import Row, DataFrame, functions as F
from utils.logging.factory import LoggerFactory
from spark.model import SparkActionResult, QueryResult


HEADER = ["failure", "min", "25th_percentile",
          "50th_percentile", "75th_percentile", "max", "count"]
SORT_LIST = ["failure"]


def exec_query(rdd: RDD[tuple]) -> QueryResult:
    # @param rdd : RDD of ['event_date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']

    def percentile_approx(values: list, p: float) -> float | None:
        """
        Approximation of percentile,
        assuming the input list is already sorted.

        :param values: List of numerical values (sorted).
        :param percent: (0-1).
        :return: Approximated percentile value.
        """

        if not values:
            return None

        n = len(values)
        product = p * n

        if product.is_integer():
            return (values[int(product) - 1] + values[int(product)]) / 2
        else:
            return values[int(product)]

    rdd_res = (
        rdd
        # Map to (serial_number, (failure, s9_power_on_hours))
        .map(lambda x: (x[1], (x[3], x[5])))
        # For each serial_number, get whether the disk has ever failed and the most recent s9_power_on_hours
        .reduceByKey(lambda x, y: (x[0] or y[0], max(x[1], y[1])))
        # Map to (failure, s9_power_on_hours)
        .map(lambda x: (x[1][0], x[1][1]))
        # Group by failure
        .groupByKey()
        # Sort the list of s9_power_on_hours
        .mapValues(lambda x: sorted(x))
        # Calculate statistics
        .map(lambda x: (
            int(x[0]),  # failure
            x[1][0],  # min
            percentile_approx(x[1], 0.25),  # 25th percentile
            percentile_approx(x[1], 0.5),  # 50th percentile
            percentile_approx(x[1], 0.75),  # 75th percentile
            x[1][-1],  # max
            len(x[1])  # count
        ))
    )

    logger = LoggerFactory.spark()
    logger.log("Starting to evaluate action of query 3..")
    start_time = time.time()
    # Triggers the action
    out_res = rdd_res.collect()
    end_time = time.time()
    logger.log("Finished evaluating..")

    res = QueryResult(name="query3", results=[SparkActionResult(
        name="query3",
        header=HEADER,
        sort_list=SORT_LIST,
        result=out_res,
        execution_time=end_time - start_time
    )])
    logger.log(f"Query 3 took {res.total_exec_time} seconds..")

    return res
