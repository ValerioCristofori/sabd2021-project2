#!/bin/bash

docker-compose up -d
	
sudo docker exec -it kafka-broker1 /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic source

echo Done!