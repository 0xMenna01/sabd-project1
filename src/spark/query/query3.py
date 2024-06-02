import time
from pyspark.rdd import RDD
from pyspark.sql import Row, DataFrame, functions as F
from utils.logging.factory import LoggerFactory
from spark.model import SparkActionResult, QueryResult

# Compute p percentile of an already sorted list


def get_percentile(p: float, l: list) -> float:
    k = p * (len(l) - 1)
    # k is an integer
    if k-int(k) == 0.0:
        k = int(k)
        return l[k]
    # k is not integer
    else:
        k = int(k)
        return (l[k]+l[k+1])/2


# ********************************** AggregateByKey components: seqFunc: (U, V) => U, combFunc: (U, U) => U) ************************************
# initial accumulator (one for each key)
start_value = []
# sequence operation: works on partitions, x is the accumulator (start_value) where we store the entire column values as a list, y is each tuple
def seq_op(x, y): return (x+[y])
# combine operation: merges the accumulators x and y from different partitions
# in this case (default 2 partitions) x is the acc for the first partition, y for the second
def comb_op(x, y): return (x+y)
# **********************************************************************************************************************************************


def exec_query(rdd: RDD[Row]) -> QueryResult:
    # @param rdd : RDD of ['event_date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']

    rdd_res = (
        rdd
        # Divide in failed and not failed
        .map(lambda x: ((x.failure, x.serial_number), (x.event_date, x.s9_power_on_hours)))
        # Reduce by taking only the records with the latest day
        .reduceByKey(lambda x, y: x if (x[0] > y[0]) else y)
        # Map to (failure, s9_power_on_hours)
        .map(lambda x: (x[0][0], x[1][1]))
        # Sorts records based on S9 column
        .sortBy(lambda x: x[1])
        # Aggregate by key to 'transpose' s9 values
        .aggregateByKey(start_value, seq_op, comb_op)
        # Map to get stats from this list: min=list[0], max, 25 percentile, 50 percentile, 75 percentile, count
        .map(lambda x: (x[0], x[1][0], x[1][len(x[1])-1], get_percentile(0.25, x[1]), get_percentile(0.5, x[1]), get_percentile(0.75, x[1]), len(x[1])))
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
        header=["failure", "min", "max", "25_percentile",
                "50_percentile", "75_percentile", "count"],
        sort_list=["failure", "min", "max", "25_percentile",
                   "50_percentile", "75_percentile", "count"],
        result=out_res,
        execution_time=end_time - start_time
    )])
    logger.log(f"Query 3 took {res.total_exec_time} seconds..")

    return res
