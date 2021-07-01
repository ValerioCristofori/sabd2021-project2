#!/bin/bash

sudo docker exec -it kafka kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic source

docker-compose down