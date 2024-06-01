import time
from pyspark.rdd import RDD
from pyspark.sql import Row, DataFrame, functions as F
from utils.logging.factory import LoggerFactory
from spark.model import SparkActionResult, QueryResult

MODELS_HEADER = ["model", "failures_count"]
MODELS_SORT_LIST = ["failures_count", "model"]

VAULTS_HEADER = ["vault_id", "failures_count", "list_of_models"]
VAULTS_SORT_LIST = ["failures_count", "vault_id"]


def exec_query_df(df: DataFrame) -> QueryResult:
    # @param df : DataFrame of ['event_date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']

    # Compute the number of failures for each (vault_id, model) pair
    partial_df = (
        df
        .filter(F.col("failure") > 0)
        .groupBy("vault_id", "model")
        .agg(F.sum("failure").alias("failures_count"))
    )
    # Cache the partial DataFrame to avoid recomputations
    partial_df.cache()

    # Compute the top 10 models with the most failures
    models_failures = (
        partial_df
        .groupBy("model")
        .agg(F.sum("failures_count").alias("model_failures"))
        .orderBy(F.desc("model_failures"))
    )

    # Compute the top 10 vaults with the most failures and for each vault, report the associated list of models
    vaults_failures = (
        partial_df
        .groupBy("vault_id")
        .agg(
            F.sum("failures_count").alias("vault_failures"),
            F.concat_ws(",", F.collect_set("model")).alias("list_of_models")
        )
        .orderBy(F.desc("vault_failures"))
    )

    logger = LoggerFactory.spark()

    logger.log("Starting to evaluate action 1/2 of query 2 with DataFrame..")
    start_time = time.time()
    # Get top 10 models with most failures
    top_models_failures = models_failures.take(30)
    end_time = time.time()
    time_action1 = end_time - start_time
    # Create the SparkActionResult
    res1 = SparkActionResult(
        name="query2_1-df-evalutaion",
        header=MODELS_HEADER,
        sort_list=MODELS_SORT_LIST,
        result=top_models_failures,
        execution_time=time_action1,
        ascending_list=[False, True]
    )

    logger.log("Starting to evaluate action 2/2 of query 2 with DataFrame..")
    start_time = time.time()
    # Get top 10 vaults with most failures
    top_vaults_failures = vaults_failures.take(30)
    end_time = time.time()
    time_action2 = end_time - start_time
    # Create the SparkActionResult
    res2 = SparkActionResult(
        name="query2_2-df-evalutaion",
        header=VAULTS_HEADER,
        sort_list=VAULTS_SORT_LIST,
        result=top_vaults_failures,
        execution_time=time_action2,
        ascending_list=[False, True]
    )

    res = QueryResult(name="query2-df-evaluation", results=[res1, res2])
    logger.log(f"Query 2 with DataFrame took {res.total_exec_time} seconds..")

    return res


def exec_query_rdd(rdd: RDD[Row]) -> QueryResult:
    # @param rdd : RDD of ['event_date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']

    # Compute the number of failures for each (vault_id, model) pairs
    partial_rdd = (
        rdd
        # Filter on failure > 0
        .filter(lambda x: x[3] > 0)
        # Convert to ((vault_id, model), failure)
        .map(lambda x: ((x[4], x[2]), x[3]))
        # Sum failures for each (vault_id, model)
        .reduceByKey(lambda acc, failure: acc + failure)
    )
    # Cache the partial RDD to avoid recomputations
    partial_rdd.cache()

    # Compute the top 10 models with the most failures
    models_failures = (
        partial_rdd
        # Convert to (model, failure)
        .map(lambda x: (x[0][1], x[1]))
        # Sum failures for each model
        .reduceByKey(lambda acc, failure: acc + failure)
        # Sort by failure count
        .sortBy(lambda x: x[1], ascending=False)
    )

    # Compute the top 10 vaults with the most failures and for each vault, report the associated list of models
    vaults_failures = (
        partial_rdd
        # Convert to (vault_id, (failure, [model]))
        .map(lambda x: (x[0][0], (x[1], [x[0][1]])))
        # Sum failures for each vault and compute the list of models (without duplicates)
        .reduceByKey(lambda a, b: (a[0] + b[0], list(set(a[1] + b[1]))))
        # Sort by failure count
        .sortBy(lambda x: x[1][0], ascending=False)
        # Convert to (vault_id, failure, list_of_models)
        .map(lambda x: (x[0], x[1][0], ','.join(x[1][1])))
    )

    logger = LoggerFactory.spark()

    logger.log("Starting to evaluate action 1/2 of query 2 with RDD..")
    start_time = time.time()
    # Get top 10 models with most failures
    # This triggers an action that persists the `partial_rdd` in memory
    top_models_failures = models_failures.take(30)
    end_time = time.time()
    time_action1 = end_time - start_time
    # Create the SparkActionResult
    res1 = SparkActionResult(
        name="query2_1-rdd-evalutaion",
        header=MODELS_HEADER,
        sort_list=MODELS_SORT_LIST,
        result=top_models_failures,
        execution_time=time_action1,
        ascending_list=[False, True]
    )

    logger.log("Starting to evaluate action 2/2 of query 2 with RDD..")
    start_time = time.time()
    # Get top 10 vaults with most failures
    # This triggers an action that reuses the persisted `partial_rdd`
    top_vaults_failures = vaults_failures.take(30)
    end_time = time.time()
    time_action2 = end_time - start_time
    # Create the SparkActionResult
    res2 = SparkActionResult(
        name="query2_2-rdd-evalutaion",
        header=VAULTS_HEADER,
        sort_list=VAULTS_SORT_LIST,
        result=top_vaults_failures,
        execution_time=time_action2,
        ascending_list=[False, True]
    )

    res = QueryResult(name="query2-rdd-evaluation", results=[res1, res2])
    logger.log(f"Query 2 with RDD took {res.total_exec_time} seconds..")

    return res
