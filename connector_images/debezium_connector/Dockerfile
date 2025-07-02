FROM debezium/connect:3.0.0.Final

USER root
ENV CONNECT_GROUP_ID=debezium-group
ENV GROUP_ID=debezium-group

# Cài đặt KeyToField SMT từ GitHub của EladLeev
RUN curl -L -o /kafka/connect/debezium-connector-mongodb/key-to-field-transform-v1.1.0.jar \
    https://github.com/EladLeev/KeyToField-smt/releases/download/v1.1.0-release/key-to-field-transform-v1.1.0.jar

COPY deploy-mongo-connector.sh /opt/deploy-mongo-connector.sh

RUN chmod +x /opt/deploy-mongo-connector.sh

CMD ["/bin/bash", "-c", "/docker-entrypoint.sh start & until curl -f http://localhost:8083/connectors >/dev/null 2>&1; do sleep 2; done && /opt/deploy-mongo-connector.sh && wait"]