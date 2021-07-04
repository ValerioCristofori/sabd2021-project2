#!/bin/bash
$KAFKA_HOME/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink-topic
$KAFKA_HOME/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query1weekly
$KAFKA_HOME/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query1monthly
$KAFKA_HOME/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query2weekly
$KAFKA_HOME/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query2monthly
$KAFKA_HOME/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query3oneHour
$KAFKA_HOME/kafka-topics.sh --delete --zookeeper localhost:2181 --topic query3twoHour


docker-compose down

# stop all flink job
JOB_ID=$(sh $FLINK_HOME/flink list | grep sabd2021-project2 |sed 's/: /\n/g' | sed -n 2p)
export JOB_ID
sh $FLINK_HOME/flink cancel "$JOB_ID"

sh $FLINK_HOME/stop-cluster.sh

echo Shutdown!