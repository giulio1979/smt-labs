#!/bin/bash
echo "Installing connector plugins"
confluent-hub install --no-prompt confluentinc/kafka-connect-http:1.5.3
echo "Installing SMTs from volume"
cp /tmp/smt/*.jar /usr/share/confluent-hub-components
#
echo "Launching Kafka Connect worker"
/etc/confluent/docker/run &
#
echo "waiting 2 minutes for things to stabilise"
sleep 120
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
    "http.api.url": "http://rest-proxy:8082/topics/test-http",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "confluent.topic.bootstrap.servers": "broker:29092",
    "confluent.topic.replication.factor": "1",
    "reporter.bootstrap.servers": "broker:29092",
    "reporter.result.topic.name": "success-responses",
    "reporter.result.topic.replication.factor": "1",
    "reporter.error.topic.name":"error-responses",
    "reporter.error.topic.replication.factor":"1",
    "transforms": "insertAppIdHeader1",
    "transforms": "insertAppIdHeader1.type": "org.apache.kafka.connect.transforms.InsertHeader",
    "transforms.insertAppIdHeader1.header": "Content-Type",
    "transforms.insertAppIdHeader1.value.literal": "application/vnd.kafka.v2+json",
    "transforms": "insertAppIdHeader2",
    "transforms": "insertAppIdHeader2.type": "org.apache.kafka.connect.transforms.InsertHeader",
    "transforms.insertAppIdHeader2.header": "Accept",
    "transforms.insertAppIdHeader2.value.literal": "application/vnd.kafka.v2+json"
    "transform": "InsertLMv1Token",
    "transform.InsertLMv1Token.ACCESS_ID": "testaccessid123",
    "transform.InsertLMv1Token.ACCESS_KEY": "testaccesskey123"
  }
}
EOF
)

curl -X POST -H "${HEADER}" --data "${DATA}" http://localhost:8083/connectors

echo "Sleeping forever"
sleep infinity
