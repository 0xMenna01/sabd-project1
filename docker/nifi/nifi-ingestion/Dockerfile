FROM apache/nifi:latest

# only if you have custom processors
#COPY template/my_nar.nar /opt/nifi/nifi-current/lib/

COPY --chown=nifi:nifi template/my-flow.xml.gz /opt/nifi/nifi-current/conf/flow.xml.gz

EXPOSE 8443

CMD ["sh", "-c", "$NIFI_HOME/bin/nifi.sh start"]
