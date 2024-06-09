import time
from pyspark.rdd import RDD
from pyspark.sql import Row, DataFrame, functions as F
from utils.logging.factory import LoggerFactory
from spark.model import SparkActionResult, QueryResult

MODELS_HEADER = ["model", "failures_count"]
MODELS_SORT_LIST = ["failures_count", "model"]

VAULTS_HEADER = ["vault_id", "failures_count", "list_of_models"]
VAULTS_SORT_LIST = ["failures_count", "vault_id"]


def exec_query(rdd: RDD[tuple]) -> QueryResult:
    # @param rdd : RDD of ['event_date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']

    # Compute the number of failures for each (vault_id, model) pairs
    partial_rdd = (
        rdd
        # Filter on failure == True
        .filter(lambda x: x[3] == True)
        # Convert to ((vault_id, model), 1)
        .map(lambda x: ((x[4], x[2]), int(1)))
        # Sum failures for each (vault_id, model)
        .reduceByKey(lambda acc, failure: acc + failure)
    )
    # Cache the partial RDD to avoid recomputations
    partial_rdd.cache()

    # Compute the top 10 models with the most failures
    models_failures = (
        partial_rdd
        # Convert to (model, failures_count)
        .map(lambda x: (x[0][1], x[1]))
        # Sum failures for each model
        .reduceByKey(lambda acc, failure: acc + failure)
        # Sort by failure count
        .sortBy(lambda x: x[1], ascending=False)  # type: ignore
    )

    # Compute the top 10 vaults with the most failures and for each vault, report the associated list of models
    vaults_failures = (
        partial_rdd
        # Convert to (vault_id, (failures_count, [model]))
        .map(lambda x: (x[0][0], (x[1], [x[0][1]])))
        # Sum failures for each vault and compute the list of models (without duplicates)
        .reduceByKey(lambda a, b: (a[0] + b[0], list(set(a[1] + b[1]))))
        # Sort by failure count
        .sortBy(lambda x: x[1][0], ascending=False)  # type: ignore
        # Convert to (vault_id, failure, list_of_models)
        .map(lambda x: (x[0], x[1][0], ','.join(x[1][1])))
    )

    logger = LoggerFactory.spark()

    logger.log("Starting to evaluate action 1/2 of query 2..")
    start_time = time.time()
    # Get top 10 models with most failures
    # This triggers an action that persists the `partial_rdd` in memory
    top_models_failures = models_failures.take(10)
    end_time = time.time()
    logger.log("Finished evaluating..")
    # Create the SparkActionResult
    res1 = SparkActionResult(
        name="query2-1",
        header=MODELS_HEADER,
        sort_list=MODELS_SORT_LIST,
        result=top_models_failures,
        execution_time=end_time - start_time,
        ascending_list=[False, True]
    )

    logger.log("Starting to evaluate action 2/2 of query 2..")
    start_time = time.time()
    # Get top 10 vaults with most failures
    # This triggers an action that reuses the persisted `partial_rdd`
    top_vaults_failures = vaults_failures.take(10)
    end_time = time.time()
    logger.log("Finished evaluating..")

    # Create the SparkActionResult
    res2 = SparkActionResult(
        name="query2-2",
        header=VAULTS_HEADER,
        sort_list=VAULTS_SORT_LIST,
        result=top_vaults_failures,
        execution_time=end_time - start_time,
        ascending_list=[False, True]
    )

    res = QueryResult(name="query2", results=[res1, res2])
    logger.log(f"Query 2 took {res.total_exec_time} seconds..")

    return res
