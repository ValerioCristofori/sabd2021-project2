#!/bin/bash
docker-compose up -d

#init conf for flink
cp ./flink/flink-conf.yaml "$FLINK_HOME/../conf/flink-conf.yaml"

sleep 3

#start flink and kafka cluster
sh $FLINK_HOME/start-cluster.sh

#create topics
$KAFKA_HOME/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic flink-topic
$KAFKA_HOME/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic query1weekly
$KAFKA_HOME/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic query1monthly
$KAFKA_HOME/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic query2weekly
$KAFKA_HOME/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic query2monthly
$KAFKA_HOME/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic query3oneHour
$KAFKA_HOME/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic query3twoHour


sleep 3

#run flink program with parallelism
sh $FLINK_HOME/flink run --parallelism 3 -d -c "it.uniroma2.main.FlinkMain" ./target/sabd2021-project2-1.0-jar-with-dependencies.jar

#wait for flink run
sleep 5

#trigger datasource
java -cp ./target/sabd2021-project2-1.0-jar-with-dependencies.jar it.uniroma2.main.DataSource


