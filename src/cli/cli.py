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
                                 "parquet", "csv", "avro"], help="The data format to use")
        self.parser.add_argument(
            "--nifi_ingestion", action="store_true", help="Whether to ingest data using NiFi")
        self.parser.add_argument(
            "--local_write", action="store_true", help="Whether to write locally")
        self.parser.add_argument(
            "--write_evaluation", action="store_true", help="Whether to write for evaluation")
        
        # Mutually exclusive group for only_process and only_pre_process
        group = self.parser.add_mutually_exclusive_group()
        group.add_argument(
            "--only_process", action="store_true", help="To only perform the data processing because preprocessed data is already available")
        group.add_argument(
            "--only_pre_process", action="store_true", help="To only perform the data preprocessing and storing it in HDFS")


    def start(self):
        args = self.parser.parse_args()
        # Get the framework
        framework = get_framework(args)
        # Get the query
        query = get_query(args)
        # Get the format
        format = get_format(args)

        if args.nifi_ingestion:
            # Schedule data ingestion
            nifi_ingestion.execute()

        sc = SparkController(
            framework, query, local_write=args.local_write, write_evaluation=args.write_evaluation)
        
        init_sc = sc.set_data_format(format)
        if args.only_pre_process:
            # Only pre-process data and store prepared data on HDFS
            init_sc.prepare_for_processing()
        elif args.only_process:
            # Only process data (because preprocessed data is already on HDFS) and store results on HDFS
            init_sc \
            .process_data() \
            .write_results()
        else:
            # Do everything at once
            init_sc \
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


def get_format(args) -> DataFormat:
    if args.format == "parquet":
        return DataFormat.PARQUET
    elif args.format == "csv":
        return DataFormat.CSV
    elif args.format == "avro":
        return DataFormat.AVRO
    else:
        raise ValueError("Invalid format")
