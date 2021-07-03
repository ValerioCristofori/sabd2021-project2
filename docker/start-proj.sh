#!/bin/bash
docker-compose up -d

#init conf for flink
cp ./flink/flink-conf.yaml "$FLINK_HOME/../conf/flink-conf.yaml"

sleep 3

#start flink and kafka cluster
sh $FLINK_HOME/start-cluster.sh

#create topics
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic flink-topic
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query1weekly
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query1monthly
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query2weekly
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query2monthly
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query3oneHour
sudo docker exec -it kafka /kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic query3twoHour

#run flink program with parallelism
sh $FLINK_HOME/flink run --parallelism 12 -d -c "it.uniroma2.main.FlinkMain" ./target/sabd2021-project2-1.0-jar-with-dependencies.jar

#wait for flink run
sleep 5

#trigger datasource
java -cp ./target/sabd2021-project2-1.0-jar-with-dependencies.jar it.uniroma2.main.DataSource


