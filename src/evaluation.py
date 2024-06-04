# A utility script that serves to evaluate the queries

import dotenv
from spark.model import DataFormat, QueryFramework, QueryNum
from spark.controller import SparkController
from ingestion import nifi_ingestion


RUNS = 10
FORMATS = [DataFormat.PARQUET, DataFormat.AVRO]


def query_evaluation():
    # Load environment variables
    dotenv.load_dotenv()

    # Execute NiFi ingestion
    nifi_ingestion.execute()

    # Evaluate queries in 10 runs
    for format in FORMATS:
        sc = SparkController(
            QueryFramework.SPARK_CORE_AND_SQL, QueryNum.QUERY_ALL, write_evaluation=True)
        sc.set_data_format(format).prepare_for_processing()
        for i in range(RUNS):
            sc.process_data().write_results()
            sc.close_session()


if __name__ == "__main__":
    query_evaluation()
