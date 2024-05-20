FROM apache/nifi:latest

# Create hadoop and ingestion conf directories
RUN mkdir /opt/nifi/nifi-current/conf/hadoop
RUN mkdir /opt/nifi/nifi-current/conf/nifi-ingestion

ENV HADOOP_HOME=/opt/nifi/nifi-current
ENV NIFI_INGESTION_HOME=/opt/nifi/nifi-current

COPY --chown=nifi:nifi conf/hadoop/core-site.xml  $HADOOP_HOME/conf/hadoop//hdfs-site.xml
COPY --chown=nifi:nifi conf/hadoop/hdfs-site.xml  $HADOOP_HOME/conf/hadoop/core-site.xml
COPY --chown=nifi:nifi conf/ingestion/.conf.json $NIFI_INGESTION_HOME/conf/nifi-ingestion/conf.json

COPY --chown=nifi:nifi template/flow.xml.gz /opt/nifi/nifi-current/conf/flow.xml.gz

EXPOSE 8443

CMD ["sh", "-c", "$NIFI_HOME/bin/nifi.sh start"]
