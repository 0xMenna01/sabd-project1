FROM apache/nifi:latest

# Create hadoop and ingestion conf directories
RUN mkdir $NIFI_HOME/conf/hadoop
RUN mkdir $NIFI_HOME/conf/nifi-ingestion

# Copy hadoop configuration files
COPY --chown=nifi:nifi conf/hadoop/core-site.xml  $NIFI_HOME/conf/hadoop//hdfs-site.xml
COPY --chown=nifi:nifi conf/hadoop/hdfs-site.xml  $NIFI_HOME/conf/hadoop/core-site.xml

# Copy the flow configuration files
COPY --chown=nifi:nifi conf/flow.xml.gz $NIFI_HOME/conf/flow.xml.gz
COPY --chown=nifi:nifi conf/flow.json.gz $NIFI_HOME/conf/flow.json.gz


EXPOSE 8443