FROM confluentinc/cp-kafka-connect:7.8.0

USER root


# Copy ClickHouse connector và Debezium SMT (nếu có)
COPY clickhouse-connector/*.jar /usr/share/java/kafka-connect-clickhouse/


# Copy script deploy connector
COPY clickhouse_connector.sh /opt/clickhouse_connector.sh
RUN chmod +x /opt/clickhouse_connector.sh

# Khởi động Kafka Connect và deploy connector sau khi REST API sẵn sàng
CMD ["/bin/bash", "-c", "\
  /etc/confluent/docker/run & \
  echo 'Waiting for Kafka Connect...'; \
  until curl -f http://localhost:8083/connectors >/dev/null 2>&1; do sleep 2; done; \
  echo 'Deploying ClickHouse Sink connector...'; \
  /opt/clickhouse_connector.sh; \
  wait"]
