#!/bin/bash

# Đợi Kafka Connect sẵn sàng
until curl -s http://localhost:8083/connectors; do
  echo ">>> Waiting for Kafka Connect REST API..."
  sleep 2
done

echo "Setting up MongoDB Connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mongo-connector",
    "config": {
      "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
      "tasks.max": "1",
      "mongodb.connection.string": "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
      "topic.prefix": "cdc",
      "mongodb.include.db.names": "test",
      "mongodb.name": "mongo_rs",
      "snapshot.mode": "initial",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false",
      
      "transforms": "unwrap, keyToField",
      "transforms.unwrap.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
      "transforms.unwrap.drop.tombstones": "true",
      "transforms.unwrap.delete.handling.mode": "rewrite",
      "transforms.unwrap.operation.header": "false",
      "transforms.unwrap.add.fields": "op,ts_ms",
      "transforms.keyToField.type": "com.github.eladleev.kafka.connect.transform.keytofield.KeyToFieldTransform"
    }
  }'
echo "MongoDB Connector setup completed!"

