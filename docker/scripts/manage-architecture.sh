#!/bin/bash

# Variables
PATH_HDFS_START_SCRIPT=/hdfs-scripts/start-dfs.sh
HADOOP_HOME=/usr/local/hadoop
HDFS_DOCKER_IMAGE=matnar/hadoop
HDFS_MASTER_CONTAINER=hdfs-master
DOCKER_NETWORK=project1-network

usage() {
    echo "Usage (only one flag):"
    echo "       ./manage-architecture.sh --start: 
                   Starts the whole architecture"
    echo "       ./manage-architecture.sh --spark-worker <number_of_workers>:
                   Starts the whole architecture with the specified number of spark workers"
    echo "       ./manage-architecture.sh -restart-dfs: 
                   Restarts the HDFS"
    echo "       ./manage-architecture.sh --hdfs-scale-out -p <port1:port2>:
                   Scales out the HDFS with a new datanode"
    echo "       ./manage-architecture.sh --scale-spark-worker <number_of_workers>:
                   Scalesthe Spark workers with the specified number of workers"
}



run_docker_compose() {
    docker-compose up --detach --scale spark-worker=$1
}

init_hdfs() {
    docker-compose exec $HDFS_MASTER_CONTAINER $PATH_HDFS_START_SCRIPT --format
}

restart_dfs() {
    docker-compose exec $HDFS_MASTER_CONTAINER $PATH_HDFS_START_SCRIPT
}

hdfs_scale_out() {
    # Get the last number of the last name node
    last_node=$(docker-compose exec $HDFS_MASTER_CONTAINER sh -c "tail -n 1 $HADOOP_HOME/etc/hadoop/workers | grep -o '[0-9]\+'")
    new_node=$((last_node + 1))
    
    # Add the new name node
    new_node_name="slave$new_node"
    docker-compose exec $HDFS_MASTER_CONTAINER sh -c "echo $new_node_name >> $HADOOP_HOME/etc/hadoop/workers"
    
    # Run the new datandode container
    docker run -t -i -p $1 -d --network=$DOCKER_NETWORK --name=$new_node_name $HDFS_DOCKER_IMAGE
    
    # Restart HDFS
    restart_dfs
}


execute() {
    if [ "$1" = "--help" ]; then
    usage
    exit 0
elif [ "$1" = "--start" ]; then
    run_docker_compose 1
    init_hdfs
elif [ "$1" = "--spark-worker" ]; then
    run_docker_compose $2
    init_hdfs
elif [ "$1" = "--restart-dfs" ]; then
    restart_dfs
elif [ "$1" = "--hdfs-scale-out" ]; then
    hdfs_scale_out $3
elif [ "$1" = "--scale-spark-worker" ]; then
    run_docker_compose $2
else
    usage
fi
}

# Execute the script
execute $@
