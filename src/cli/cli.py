import argparse
from spark.model import QueryFramework, QueryNum, DataFormat
from spark.controller import SparkController


class Cli:
    def __init__(self):
        self.parser = argparse.ArgumentParser(
            description="CLI for Spark queries")
        self.parser.add_argument("framework", choices=[
                                 "spark-core", "spark-sql", "spark-all"], help="The Spark framework to use")
        self.parser.add_argument("query", choices=[
                                 "query1", "query2", "query3", "query-all"], help="The query number to perform")
        self.parser.add_argument("format", choices=[
                                 "format-parquet", "format-csv", "format-json", "format-avro", "format-all"], help="The data format to use")

    def start(self):
        args = self.parser.parse_args()
        # Get the framework
        framework = get_framework(args)
        # Get the query
        query = get_query(args)
        # Get the formats
        formats = get_formats(args)

        spark = SparkController(framework, query)
        for format in formats:
            (
                spark
                .set_data_format(format)
                .prepare_for_processing()
                .process_data()
            )


def get_framework(args) -> QueryFramework:
    if args.framework == "spark-core":
        return QueryFramework.SPARK_CORE
    elif args.framework == "spark-sql":
        return QueryFramework.SPARK_SQL
    else:
        return QueryFramework.SPARK_CORE_AND_SQL


def get_query(args) -> QueryNum:
    if args.query == "query1":
        return QueryNum.QUERY_ONE
    elif args.query == "query2":
        return QueryNum.QUERY_TWO
    elif args.query == "query3":
        return QueryNum.QUERY_THREE
    else:
        return QueryNum.QUERY_ALL


def get_formats(args) -> list[DataFormat]:
    if args.format == "format-parquet":
        return [DataFormat.PARQUET]
    elif args.format == "format-csv":
        return [DataFormat.CSV]
    elif args.format == "format-json":
        return [DataFormat.JSON]
    elif args.format == "format-avro":
        return [DataFormat.AVRO]
    else:
        return [
            DataFormat.PARQUET,
            DataFormat.CSV,
            DataFormat.JSON,
            DataFormat.AVRO
        ]
