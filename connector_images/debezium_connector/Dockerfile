FROM debezium/connect:3.0.0.Final

USER root

# Tạo thư mục cho các plugin
RUN mkdir -p /kafka/connect/debezium-connector-mongodb/

# Tải Apicurio Registry Avro Converter (fat jar)
RUN curl -L -o /kafka/connect/debezium-connector-mongodb/apicurio-registry-utils-converter-3.0.9-fat.jar \
    https://repo1.maven.org/maven2/io/apicurio/apicurio-registry-utils-converter/3.0.9/apicurio-registry-utils-converter-3.0.9-fat.jar

# Tải KeyToField SMT
RUN curl -L -o /kafka/connect/debezium-connector-mongodb/key-to-field-transform-v1.1.0.jar \
    https://github.com/EladLeev/KeyToField-smt/releases/download/v1.1.0-release/key-to-field-transform-v1.1.0.jar

# Copy script tạo connector MongoDB
COPY deploy-mongo-connector.sh /opt/deploy-mongo-connector.sh
RUN chmod +x /opt/deploy-mongo-connector.sh

# Khởi động Debezium Connect và tự động deploy connector Mongo
CMD ["/bin/bash", "-c", "echo '🧩 Plugin path: /kafka/connect/'; ls -la /kafka/connect/debezium-connector-mongodb/; \
  /docker-entrypoint.sh start & \
  until curl -f http://localhost:8083/connectors >/dev/null 2>&1; do sleep 2; done && \
  /opt/deploy-mongo-connector.sh && wait"]
