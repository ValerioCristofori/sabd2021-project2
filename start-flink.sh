#!/bin/bash

sudo docker exec -it jobmanager flink run -c it.uniroma2.main.FlinkMain ./data/sabd2021-project2-1.0-jar-with-dependencies.jar
