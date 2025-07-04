#!/bin/bash
echo ">>> Setting up ClickHouse Sink Connector..."

# Đợi Kafka Connect sẵn sàng
until curl -s http://localhost:8083/connectors; do
  echo ">>> Waiting for Kafka Connect REST API..."
  sleep 2
done

# Gửi POST để tạo connector
curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d '{
    "name":"clickhouse-sink-connector",
    "config":{
      "connector.class":"com.clickhouse.kafka.connect.ClickHouseSinkConnector",
      "tasks.max":"6",
      "topics":"cdc.test.accounts",
      "hostname":"clickhouse",
      "port":"8123",
      "database":"default",
      "username":"default",
      "password":"giaphu",
      "auto.create.tables":"false",
      "auto.create.partitions":"true",
      "topic.creation.default.partitions": "3",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false"
    }
  }'

echo ">>> ClickHouse Sink Connector setup completed."


