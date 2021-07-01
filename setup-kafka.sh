#!/bin/bash

docker-compose up -d

sleep 3

sudo docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic source