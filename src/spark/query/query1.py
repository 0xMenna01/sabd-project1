import time
from pyspark.rdd import RDD
from pyspark.sql import Row, DataFrame, functions as F


def exec_query_from_rdd(rdd: RDD[Row]) -> tuple[RDD, float]:
    # @param rdd : RDD of ['date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']

    lookup_failures = [2, 3, 4]
    broadcast_lookup_failures = rdd.context.broadcast(set(lookup_failures))

    # Process the RDD
    res_rdd = (
        rdd
        .filter(lambda x: x.failure > 0)  # Early filter to reduce data size
        # Convert to ((date, vault_id), failure)
        .map(lambda x: ((x.date, x.vault_id), x.failure))
        # Sum failures per (date, vault_id)
        .reduceByKey(lambda acc, failure: acc + failure)
        # Filter based on lookup failures
        .filter(lambda x: x[1] in broadcast_lookup_failures.value)
    )

    # Trigger the action and evaluate the time
    start_time = time.time()
    result = res_rdd.collect()
    end_time = time.time()

    return (res_rdd, end_time - start_time)


def exec_query_from_df(df: DataFrame) -> tuple[DataFrame, float]:
    # @param df: DataFrame with columns ['date', 'serial_number', 'model', 'failure', 'vault_id', 's9_power_on_hours']

    lookup_failures = [2, 3, 4]
    broadcast_lookup_failures = df.sparkSession.sparkContext.broadcast(
        set(lookup_failures))

    df_result = (
        df
        .filter(df.failure > 0)
        .groupBy("date", "vault_id")
        .agg(F.sum("failure"))
        .filter(F.col("failure").isin(broadcast_lookup_failures.value))
    )

    # Trigger the action and evaluate the time
    start_time = time.time()
    df_result.collect()
    end_time = time.time()

    return (df_result, end_time - start_time)
