FROM apache/nifi:latest

ENV HADOOP_HOME=/opt/nifi/nifi-current/conf/hadoop

# only if you have custom processors
#COPY template/my_nar.nar /opt/nifi/nifi-current/lib/
#COPY template/testflow1.xml /opt/nifi/nifi-current/conf/templates

COPY hadoop/core-site.xml  $HADOOP_HOME/hdfs-site.xml
COPY hadoop/hdfs-site.xml  $HADOOP_HOME/core-site.xml

COPY --chown=nifi:nifi template/my-flow.xml.gz /opt/nifi/nifi-current/conf/flow.xml.gz

EXPOSE 8443

CMD ["sh", "-c", "$NIFI_HOME/bin/nifi.sh start"]
