#!/bin/bash


run_docker_compose() {
    docker-compose up --scale hdfs-datanode=$1 --scale spark-worker=$2
}

start_nodes() {
    # Default scaling parameters
    datanode=1
    spark_worker=1
    
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --datanode)
                datanode=$2
                shift 2
                ;;
            --spark-worker)
                spark_worker=$2
                shift 2
                ;;
            *)
                echo "Invalid argument: $1"
                exit 1
                ;;
        esac
    done

    # Run Docker Compose with scaling parameters
    run_docker_compose $datanode $spark_worker
}

# Execute `start_nodes` function
start_nodes "$@"
