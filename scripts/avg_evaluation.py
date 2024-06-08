# Given the evaluation results at `Results/evaluation.csv`, this utility script saves a new csv file with the average execution time for each query, given a data format and number of spark workers. Then, it generates plots of the new average evaluations results at `Results/average_evaluation.csv`. Plots are saved to `Results/plots` directory.

import pandas as pd
import matplotlib.pyplot as plt
from loguru import logger
import os

# Define the input and output file names
EVAL_PATH = 'Results/evaluation.csv'
AVG_EVAL_PATH = 'Results/average_evaluation.csv'
PLOTS_PATH = 'Results/plots'
QUERIES = [['query1', 'sql-query1'],
           ['query2', 'sql-query2'], ['query3', 'sql-query3']]
FORMATS = ['parquet', 'avro']
WORKER_NODES = [1, 2, 3, 4]


def avg_evaluation_plots():
    if not os.path.exists(PLOTS_PATH):
        os.makedirs(PLOTS_PATH)
        
    df = pd.read_csv(EVAL_PATH)
    # Group by query and format, then calculate the mean execution time
    avg_exec_times = df.groupby(['query', 'format', 'worker_nodes'])[
        'execution_time'].mean().reset_index()
    avg_exec_times.to_csv(AVG_EVAL_PATH, index=False)
    logger.info(f'Average evaluation results saved to {AVG_EVAL_PATH}')

    for queries in QUERIES:
        plt.figure(figsize=(10, 6))
        for query in queries:
            for fmt in FORMATS:
                query_fmt_data = avg_exec_times[(avg_exec_times['query'] == query) & (
                    avg_exec_times['format'] == fmt)]
                plt.plot(query_fmt_data['worker_nodes'],
                         query_fmt_data['execution_time'], label=f'{query} ({fmt})')

        # Adding details to the plot
        plt.title(f'Average Execution Time for {queries[0]} and {queries[1]}')
        plt.xlabel('Number of Worker Nodes')
        plt.ylabel('Average Execution Time (seconds)')
        plt.xticks(WORKER_NODES)
        plt.legend()
        plt.grid(True)
        plt.savefig(f'{PLOTS_PATH}/{queries[0]}_avg_exec_time_plot.png')
        logger.info(f'Plot for {queries[0]} saved.')


if __name__ == "__main__":
    avg_evaluation_plots()
