import time
from pyspark.sql import DataFrame, functions as F
from api.spark import SparkAPI
from utils.logging.factory import LoggerFactory
from spark.model import SparkActionResult, QueryResult
from spark.query.query2 import MODELS_HEADER, MODELS_SORT_LIST, VAULTS_HEADER, VAULTS_SORT_LIST


def exec_query(df: DataFrame) -> QueryResult:
    session = SparkAPI.get().session
    # Compute the number of failures for each (vault_id, model) pair
    partial_df = session.sql("""
        SELECT vault_id, model, SUM(failure) AS failures_count
        FROM DisksMonitor
        WHERE failure > 0
        GROUP BY vault_id, model
    """)
    partial_df.createOrReplaceTempView("VaultModelFailures")

    # Compute the top 10 models with the most failures
    models_failures = session.sql("""
        SELECT model, SUM(failures_count) AS model_failures
        FROM VaultModelFailures
        GROUP BY model
        ORDER BY model_failures DESC
        LIMIT 10
    """)

    # Compute the top 10 vaults with the most failures and for each vault, report the associated list of models
    vaults_failures = session.sql("""
    SELECT vault_id, 
           SUM(failures_count) AS vault_failures, 
           CONCAT_WS(',', COLLECT_SET(model)) AS list_of_models
    FROM VaultModelFailures
    GROUP BY vault_id
    ORDER BY vault_failures DESC
    LIMIT 10
    """)

    logger = LoggerFactory.spark()

    logger.log("Starting to evaluate action 1/2 of SQL query 2..")
    start_time = time.time()
    top_models_failures = models_failures.collect()
    end_time = time.time()
    time_action1 = end_time - start_time

    res1 = SparkActionResult(
        name="sql-query2-1",
        header=MODELS_HEADER,
        sort_list=MODELS_SORT_LIST,
        result=top_models_failures,
        execution_time=time_action1,
        ascending_list=[False, True]
    )

    logger.log("Starting to evaluate action 2/2 of SQL query 2..")
    start_time = time.time()
    top_vaults_failures = vaults_failures.collect()
    end_time = time.time()
    time_action2 = end_time - start_time

    res2 = SparkActionResult(
        name="sql-query2-2",
        header=VAULTS_HEADER,
        sort_list=VAULTS_SORT_LIST,
        result=top_vaults_failures,
        execution_time=time_action2,
        ascending_list=[False, True]
    )

    res = QueryResult(name="sql-query2", results=[res1, res2])
    logger.log(f"SQL Query 2 took {res.total_exec_time} seconds..")

    return res
