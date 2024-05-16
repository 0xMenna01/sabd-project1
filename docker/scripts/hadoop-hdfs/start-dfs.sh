#!/bin/sh

WORKERS_FILE=$HADOOP_HOME/etc/hadoop/workers

if [ "$1" = "--format" ]; then
    # Before doing anything, just mantain slave1 in the workers file, delete the rest
    head -n 1 $WORKERS_FILE > tmpfile && mv tmpfile $WORKERS_FILE
    # Format
    hdfs namenode -format
fi

$HADOOP_HOME/sbin/start-dfs.sh