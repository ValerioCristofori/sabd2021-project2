#!/bin/bash
docker-compose up -d

sleep 1

sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic flink-topic
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query1weekly
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query1monthly
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query2weekly
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query2monthly
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query3oneHour
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query3twoHour

echo Done!
