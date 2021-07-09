#!/bin/bash
sudo docker exec -it kafka ./kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic flink-topic
sudo docker exec -it kafka ./kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic query1weekly
sudo docker exec -it kafka ./kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic query1monthly
sudo docker exec -it kafka ./kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic query2weekly
sudo docker exec -it kafka ./kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic query2monthly
sudo docker exec -it kafka ./kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic query3oneHour
sudo docker exec -it kafka ./kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic query3twoHour

docker-compose down

# stop all flink job
JOB_ID=$(sh $FLINK_HOME/flink list | grep sabd2021-project2 |sed 's/: /\n/g' | sed -n 2p)
export JOB_ID
sh $FLINK_HOME/flink cancel "$JOB_ID"

sh $FLINK_HOME/stop-cluster.sh

echo Shutdown!