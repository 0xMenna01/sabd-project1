import csv
import os
from api.spark import SparkAPI
from pyspark.sql import DataFrame
from utils.config.factory import ConfigFactory

EVAL_PATH = os.path.join(os.getcwd(), "Results", "evaluation.csv")


def create_dir(path: str) -> None:
    """Create a directory if it does not exist."""
    if not os.path.exists(path):
        os.makedirs(path)


def load_bytes_from(path: str) -> bytes:
    """Load the content of a file."""
    with open(path, 'rb') as file:
        # Read the entire file
        content = file.read()

    return content


def write_file(path: str, content: str) -> None:
    """Write content to a file."""
    with open(path, "w") as file:
        file.write(content)


def delete_file(path: str) -> None:
    """Delete a file if it exists."""
    if os.path.exists(path):
        os.remove(path)


QUERY_RESULTS_PATH = os.path.join(
    os.getcwd(), "Results")


def results_path_from_filename(filename: str) -> str:
    return os.path.join(QUERY_RESULTS_PATH, filename)


def write_result_as_csv(res_df: DataFrame, out_path: str) -> None:
    header = res_df.schema.names
    res_list = res_df.rdd.collect()

    if (not os.path.exists(out_path)):
        # Create file and change access mode
        with open(out_path, "+x") as out_file:
            pass

    with open(out_path, "+w") as out_file:
        writer = csv.writer(out_file)
        writer.writerow(header)
        writer.writerows(res_list)


def write_evaluation(query_name: str, format: str, exec_time: float) -> None:

    worker_nodes = SparkAPI.get().context._jsc.sc(  # type: ignore
    ).getExecutorMemoryStatus().size() - 1  # type: ignore
    eval_path = results_path_from_filename("evaluation.csv")

    if (not os.path.exists(eval_path)):
        with open(eval_path, "+x") as out_file:
            writer = csv.writer(out_file)
            writer.writerow(
                ["query", "format", "worker_nodes", "execution_time"])

    with open(eval_path, "a") as out_file:
        writer = csv.writer(out_file)
        writer.writerow([query_name, format, worker_nodes, exec_time])


def delete_evaluation_file() -> None:
    delete_file(EVAL_PATH)
