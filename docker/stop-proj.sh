#!/bin/bash
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic flink-topic
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic query1weekly
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic query1monthly
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic query2weekly
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic query2monthly
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic query3oneHour
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic query3twoHour

sh $FLINK_HOME/stop-cluster.sh

docker-compose down

echo Shutdown!