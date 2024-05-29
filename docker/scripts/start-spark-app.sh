#!/bin/bash

docker build ./client -t spark-client
cd ../
docker run -t -i -p 4040:4040 --network project1-network --name=spark-app --volume ./src:/home/spark/src --volume ./Results:/home/spark/Results --workdir /home/spark spark-client

