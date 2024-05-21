#!/bin/sh

hdfs dfs -mkdir /dataset
# This is does the best security practice,
# because it does not respect the principle of least privilege.
# Security can be introduced at the network level.
hdfs dfs -chmod 777 /dataset
