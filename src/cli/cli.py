import argparse
import time
from spark.model import QueryFramework, QueryNum, DataFormat
from spark.controller import SparkController
from ingestion import nifi_ingestion
from api.spark import SparkAPI

class Cli:
    def __init__(self):
        self.parser = argparse.ArgumentParser(
            description="CLI for Spark queries")
        self.parser.add_argument("framework", choices=[
                                 "spark-core", "spark-sql", "spark-all"], help="The Spark framework to use")
        self.parser.add_argument("query", choices=[
                                 "1", "2", "3", "all"], help="The query number to perform")
        self.parser.add_argument("format", choices=[
                                 "parquet", "csv", "avro", "all"], help="The data format to use")
        self.parser.add_argument(
            "--nifi_ingestion", action="store_true", help="Whether to ingest data using NiFi")
        self.parser.add_argument(
            "--local_write", action="store_true", help="Whether to write locally")
        self.parser.add_argument(
            "--write_evaluation", action="store_true", help="Whether to write for evaluation")

    def start(self):
        args = self.parser.parse_args()
        # Get the framework
        framework = get_framework(args)
        # Get the query
        query = get_query(args)
        # Get the formats
        formats = get_formats(args)

        if args.nifi_ingestion:
            # Schedule data ingestion
            nifi_ingestion.execute()

        api = SparkAPI.get()
        spark = SparkController(
            framework, query, local_write=args.local_write, write_evaluation=args.write_evaluation)
        for format in formats:
            spark \
                .set_data_format(format) \
                .prepare_for_processing() \
                .process_data() \
                .write_results()


def get_framework(args) -> QueryFramework:
    if args.framework == "spark-core":
        return QueryFramework.SPARK_CORE
    elif args.framework == "spark-sql":
        return QueryFramework.SPARK_SQL
    elif args.framework == "spark-all":
        return QueryFramework.SPARK_CORE_AND_SQL
    else:
        raise ValueError("Invalid framework")


def get_query(args) -> QueryNum:
    if args.query == "1":
        return QueryNum.QUERY_ONE
    elif args.query == "2":
        return QueryNum.QUERY_TWO
    elif args.query == "3":
        return QueryNum.QUERY_THREE
    elif args.query == "all":
        return QueryNum.QUERY_ALL
    else:
        raise ValueError("Invalid query")


def get_formats(args) -> list[DataFormat]:
    if args.format == "parquet":
        return [DataFormat.PARQUET]
    elif args.format == "csv":
        return [DataFormat.CSV]
    elif args.format == "avro":
        return [DataFormat.AVRO]
    elif args.format == "all":
        return [DataFormat.PARQUET, DataFormat.CSV, DataFormat.AVRO]
    else:
        raise ValueError("Invalid format")
