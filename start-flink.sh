#!/bin/bash
sudo docker exec -it jobmanager ./bin/start-cluster.sh
sudo docker exec -it jobmanager ./bin/flink run -c it.uniroma2.main.FlinkMain /data/sabd2021-project2-1.0-jar-with-dependencies.jar
