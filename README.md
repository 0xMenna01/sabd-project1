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

### Starting the Overall Architecture

To start the overall architecture, you need to run the script located at `docker/scripts/manage-architecture.sh`. Follow these steps:

1. Move to the `docker` directory:

   ```bash
   cd docker
   ```

2. To initialize the architecture with a local dataset acquisition configuration for NiFi, run the script with the dataset path parameter:

   ```bash
   ./scripts/manage-architecture.sh --start --dataset <dataset_path>
   ```

3. Alternatively, to start the architecture with a remote dataset acquisition configuration, run:

   ```bash
   ./scripts/manage-architecture.sh --start
   ```

**Note:** The remote configuration allows only one download per day, so only one ingestion can be performed daily with this setup.

For further details of the script, such as scaling parameters and changing the dataset acquisition configuration for nifi (to be performed only after the architecture has been started with one of the commands above), execute:

```bash
./scripts/manage-architecture.sh --help
```

### Running the Spark Client

Once the architecture has started, to run queries using the Spark client, follow these steps:

1. Ensure you are in the `docker` folder:

   ```bash
   cd docker
   ```

2. Start the Spark client container by running:

   ```bash
   ./scripts/start-spark-app.sh
   ```

   This command will provide you with a shell into the client container.

3. To run the Spark client and execute queries, use the src/main.py program along with the necessary parameters. For instance, if it's your first time running the client and you wish to execute all queries, you can use the following command:

   ```bash
   python src/main.py spark-all all parquet --nifi-ingestion
   ```

   This command will execute queries with both Spark Core and Spark SQL (`spark-all`), executing all queries (`all`) using the Parquet format (`parquet`), and scheduling dataset ingestion through NiFi (`--nifi-ingestion`). It will also perform both preprocessing and processing tasks.

   - To only perform preprocessing or processing tasks, use the `--pre-process` or `--process` flags respectively.

For the full usage details of the script, run:

```bash
python src/main.py --help
```

**Note:** You must execute the script from the Spark user's home directory.
**Note:** If you intend to write the evaluation results locally (with the flag --write-evaluation), we recommend deleting the evaluation.csv file in the Results directory beforehand, as the program will append the computed evaluation to that file rather than overwriting it.

### Stopping the Spark Client

To stop the Spark client, execute the following command from the docker folder:

```sh
./scripts/kill-spark-app.sh
```

### Stopping the Architecture

To stop the architecture, navigate to the `docker` folder and run the provided script:

```sh
./scripts/stop-architecture.sh
```
