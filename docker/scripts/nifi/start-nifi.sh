#!/bin/bash

NIFI_PROPERTIES_PATH=$NIFI_HOME/conf/nifi.properties
NIFI_INGESTION_CONF=$NIFI_HOME/conf/nifi-ingestion
SENSITIVE_KEY=$(cat "$NIFI_INGESTION_CONF/scripts/.sensitive-key")
CONFIG_LOCAL_DATASET=$NIFI_INGESTION_CONF/scripts/local-conf.json
CONFIG_REMOTE_DATASET=$NIFI_INGESTION_CONF/scripts/remote-conf.json

# Update nifi.sensitive.props.key
sed -i "s/^nifi.sensitive.props.key=.*/nifi.sensitive.props.key=$SENSITIVE_KEY/" $NIFI_PROPERTIES_PATH


if [ "$1" = "--local" ]; then
    cat $CONFIG_LOCAL_DATASET > $NIFI_INGESTION_CONF/conf.json
    
elif [ "$1" = "--remote" ]; then
    cat $CONFIG_REMOTE_DATASET > $NIFI_INGESTION_CONF/conf.json
else
    echo "Usage:"
    echo "       ./start-nifi --local: 
                    Starts nifi with a local dataset"
    echo "       ./start-nifi --remote: 
                    Starts nifi with a remote dataset"
    exit 1
fi

# Start nifi
$NIFI_HOME/bin/nifi.sh start