#!/bin/bash

# Variables
PATH_HDFS_SCRIPTS=/hdfs-scripts
PATH_NIFI_SCRIPTS=/opt/nifi/nifi-current/conf/nifi-ingestion/scripts
HADOOP_HOME=/usr/local/hadoop
HDFS_DOCKER_IMAGE=matnar/hadoop
HDFS_MASTER_CONTAINER=hdfs-master
NIFI_CONTAINER=nifi
DOCKER_NETWORK=project1-network

DATASET_RELATIVE_PATH=./nifi/nifi-ingestion/dataset

usage() {
    echo "Usage:"
    echo "       ./manage-architecture.sh --start [--dataset <dataset_path>]:
                   Starts the whole architecture. 
                   If the dataset path is provided, the acquisition will be local, otherwise it will be fetched from a remote backblaze bucket (only one download per day)"
    echo "       ./manage-architecture.sh --restart-dfs: 
                   Restarts the HDFS"
    echo "       ./manage-architecture.sh --hdfs-scale-out -p <port1:port2>:
                   Scales out the HDFS with a new datanode"
    echo "       ./manage-architecture.sh --scale-spark-worker <number_of_workers>:
                   Scales the Spark workers with the specified number of workers"
    echo "       ./manage-architecture.sh --nifi-remote: 
                   Sets Nifi to acquire the dataset remotely"
    echo "       ./manage-architecture.sh --nifi-local:
                     Sets Nifi to acquire the dataset locally"
}

copy_dataset() {
    local dataset_path=$1
    
   # Check if the input path exists and is a file
    if [ ! -f "$dataset_path" ]; then
        echo "Input path does not exist or is not a file."
        exit 1
    fi
    
    # Create the destination directory if it doesn't exist
    mkdir -p "$DATASET_RELATIVE_PATH"
    
    echo "Copying the dataset to the desired directory, required for the Docker volume..."
    # Copy the dataset file to the destination directory
    cp -r $dataset_path $DATASET_RELATIVE_PATH/raw_data_medium-utv_sorted.csv
    
    echo "Dataset has been copied to docker volume $DATASET_RELATIVE_PATH, successfully."
}

run_docker_compose() {
    echo "Starting the architecture with $1 Spark workers..."
    docker compose up --detach --scale spark-worker=$1
}

init_hdfs() {
    echo "Initializing HDFS..."
    docker compose exec $HDFS_MASTER_CONTAINER $PATH_HDFS_SCRIPTS/start-dfs.sh --format
    docker compose exec $HDFS_MASTER_CONTAINER $PATH_HDFS_SCRIPTS/init-dataset-directory.sh
}

restart_dfs() {
    echo "Restarting HDFS..."
    docker compose exec $HDFS_MASTER_CONTAINER $PATH_HDFS_SCRIPTS/start-dfs.sh
}

hdfs_scale_out() {
    echo "Scaling out HDFS datanodes..."
    # Get the last number of the last name node
    last_node=$(docker compose exec $HDFS_MASTER_CONTAINER sh -c "tail -n 1 $HADOOP_HOME/etc/hadoop/workers | grep -o '[0-9]\+'")
    new_node=$((last_node + 1))
    
    # Add the new name node
    new_node_name="slave$new_node"
    docker compose exec $HDFS_MASTER_CONTAINER sh -c "echo $new_node_name >> $HADOOP_HOME/etc/hadoop/workers"
    
    # Run the new datandode container
    docker run -t -i -p $1 -d --network=$DOCKER_NETWORK --name=$new_node_name $HDFS_DOCKER_IMAGE
    
    # Restart HDFS
    restart_dfs
}

config_nifi() {
    echo "Configuring Nifi..."
    docker compose exec $NIFI_CONTAINER $PATH_NIFI_SCRIPTS/set-config.sh $1
}

execute() {
    if [ "$1" = "--help" ]; then
        usage
        exit 0
    elif [ "$1" = "--start" ]; then
        if [ "$2" = "--dataset" ] && [ -n "$3" ]; then
            copy_dataset $3
            run_docker_compose 1
            init_hdfs
            config_nifi --local
        else
            run_docker_compose 1
            init_hdfs
            config_nifi --remote
        fi
    elif [ "$1" = "--restart-dfs" ]; then
        restart_dfs
    elif [ "$1" = "--hdfs-scale-out" ]; then
        hdfs_scale_out $3
    elif [ "$1" = "--scale-spark-worker" ]; then
        run_docker_compose $2
    elif [ "$1" = "--nifi-remote" ]; then
        config_nifi --remote
    elif [ "$1" = "--nifi-local" ]; then
        config_nifi --local
    else
        usage
    fi
}

# Execute the script
execute $@
