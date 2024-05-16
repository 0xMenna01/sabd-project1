#!/bin/bash

docker build ../dockerfiles -f query-executor.Dockerfile -t query-executor
cd ../
docker run -t -i -p 4040:4040 --network sadb_network --name=executor query-executor
