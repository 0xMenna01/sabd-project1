#!/bin/bash

NIFI_CONTAINER=nifi
NIFI_INGESTION_HOME=/opt/nifi/nifi-current/conf/nifi-ingestion
TMP_CONF=tmp-conf.json

usage() {
    echo "Usage:"
    echo "       ./ingestion-config.sh <path_to_config>: 
                   Sets the ingestion congifuration"
}

nifi_dev_mode() {
    if 
    docker cp <local_path> $NIFI_INGESTION_HOME/conf.json
}

execute() {
    if [ "$1" = "--help" ]; then
    usage
    exit 0
elif [ "$1" = "--dev" ]; then
    ## Set the ingestion in development mode
elif [ "$1" = "--prod" ]; then
    ## Set the ingestion in production mode
else
    # default to dev mode
fi
}

# Execute the script
execute $@
