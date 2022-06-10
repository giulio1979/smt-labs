#!/bin/bash
echo "Installing connector plugins"
confluent-hub install --no-prompt confluentinc/kafka-connect-http:1.5.3
confluent-hub install --no-prompt confluentinc/connect-transforms:1.4.3
echo "Installing SMTs from volume"
cp /tmp/smt/*.jar /usr/share/confluent-hub-components
#
echo "Launching Kafka Connect worker"
/etc/confluent/docker/run &
#
echo "waiting 2 minutes for things to stabilise"
sleep 60
echo "Starting HTTP Sink Connector"

HEADER="Content-Type: application/json"

DATA=$(
  cat <<EOF
{
  "name": "HttpSink",
  "config": {
    "topics": "http-messages",
    "tasks.max": "1",
    "connector.class": "io.confluent.connect.http.HttpSinkConnector",
    "http.api.url": "http://rest-proxy:8082/topics/rest-result",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "confluent.topic.bootstrap.servers": "broker:29092",
    "confluent.topic.replication.factor": "1",
    "headers": "Content-Type:application/vnd.kafka.json.v2+json",
    "request.body.format": "json",
    "batch.json.as.array": "true",
    "reporter.result.topic.name": "",
    "reporter.error.topic.name": "",
    "transforms": "HoistField,InsertField,InsertLMv1Token",
    "transforms.HoistField.type": "org.apache.kafka.connect.transforms.HoistField$Value",
    "transforms.HoistField.field": "msg",
    "transforms.InsertField.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.InsertField.static.field": "_lm.resourceid",
    "transforms.InsertField.static.value": "{\"system.deviceid\": \"41\"}",
    "transforms.InsertLMv1Token.type": "work.hashi.kafka.smtLabs.InsertLMv1Token",
    "transforms.InsertLMv1Token.access.id": "testaccessid123",
    "transforms.InsertLMv1Token.access.key": "testaccesskey123"
  }
}
EOF
)

curl -X POST -H "${HEADER}" --data "${DATA}" http://localhost:8083/connectors

echo "Sleeping forever"
sleep infinity
