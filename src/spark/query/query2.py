import time
from pyspark.rdd import RDD
from pyspark.sql import Row, DataFrame, functions as F
from utils.logging.factory import LoggerFactory


def exec_query(rdd: RDD[Row]) -> tuple[tuple[RDD, RDD], float]:
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
    )

    logger = LoggerFactory.spark()
    logger.log("Starting to evaluate query 2..")
    start_time = time.time()
    # Triggers two actions on the resulting RDDs
    # Get top 10 models with most failures
    models_failures.take(10)
    # Get top 10 vaults with most failures
    vaults_failures.take(10)
    end_time = time.time()

    return ((models_failures, vaults_failures), end_time - start_time)
