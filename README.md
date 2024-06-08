# Data Monitoring Analysis with Apache Spark

![Python](https://img.shields.io/badge/Python-v3.11-blue.svg?logo=python&longCache=true&logoColor=white&colorB=5e81ac&style=flat-square&colorA=4c566a)
![GitHub Last Commit](https://img.shields.io/github/last-commit/google/skia.svg?style=flat-square&colorA=4c566a&colorB=a3be8c&logo=GitHub)

The purpose of this project is to utilize the Apache Spark data processing framework to answer queries on telemetry data from approximately 200k hard disks in data centers managed by Backblaze. For the purposes of this project, a reduced version of the dataset indicated in the ACM DEBS 2024 Grand Challenge is provided.

The dataset contains S.M.A.R.T monitoring data, extended with some attributes captured by Backblaze. It includes events regarding around 200k hard disks, where each event reports the S.M.A.R.T. status of a particular hard disk on a specific day. The reduced dataset contains approximately 3 million events (compared to the 5 million in the original dataset).

## Project Queries

#### Q1
- For each day, for each vault (refer to the vault id field), calculate the total number of failures. Determine the list of vaults that have experienced exactly 4, 3, and 2 failures.

#### Q2
- Calculate the top 10 models of hard disks that have experienced the highest number of failures. The ranking should include the hard disk model and the total number of failures suffered by hard disks of that specific model. Then, compute a second ranking of the top 10 vaults that have recorded the highest number of failures. For each vault, report the number of failures and the list (without repetitions) of hard disk models subject to at least one failure.

#### Q3
- Compute the minimum, 25th, 50th, 75th percentile, and maximum of the operating hours (s9 power on hours field) of hard disks that have experienced failures and hard disks that have not experienced failures. Pay attention, the s9 power on hours field reports a cumulative value, therefore the statistics required by the query should refer to the last useful detection day for each specific hard disk (consider the use of the serial number field). Also, indicate the total number of events used for calculating the statistics in the output.


## Usage

1. When running the system for the first time:
   Inside ./docker/ run the following command:
   - ./scripts/manage-architecture.sh --start [--dataset <dataset_path>] ['--help']
3. When the environment is setup, start the spark application with the following command:
   - ./scripts/start-spark-app.sh 
5. Once in the spark container run:
   - python src/main.py [-h] [--nifi-ingestion] [--local-write] [--write-evaluation] [--process | --pre-process] {spark-core,spark-sql,spark-all} {1,2,3,all} {parquet,avro}


