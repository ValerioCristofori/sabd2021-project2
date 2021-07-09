# sabd2021-project2
The aim of the project is to answer some queries regarding a dataset relating to data coming from Automatic Identification System (AIS) devices, using the Apache Flink framework or, alternatively, Apache Storm.

## Query1
Calculate for each cell of the Western Mediterranean Sea2, the daily average number of military ships (SHIPTYPE = 35), passenger ships (SHIPTYPE = 60-69), cargo ships (SHIPTYPE = 70-79) and others (all ships who do not have a SHIPTYPE that falls within the previous cases) in the last 7 days (of event time) and 1 month (of event time)

## Query2
For the Western and Eastern Mediterranean Sea3 provide the ranking of the three most frequented cells in the two service hours 00: 00-11: 59 and 12: 00-23: 59. In a given time slot, the degree of attendance of a cell is calculated as the number of different ships that cross the cell in the time slot in question.

## Query3
Provide the real-time ranking of the 5 trips with the highest mileage score. The mileage score is calculated as the distance traveled up to that time of the journey. For the calculation of the distance, consider the Euclidean distance. As a starting point, consider the first pair (LAT, LON) registered for the trip under consideration.


# Installation and Usage

## Requirements
Be sure you have Flink installed and set the env variable $FLINK_HOME to absolute path to flink scripts
(e.g. /path/to/flink-${version}/bin )

Clone the repository and in the directory of Makefile

```
sudo make build
sudo make up
```

For building the .jar , setup the docker-compose env and start Datasource.java

In another shell you can see the output of the queries with

```
sh lauchMonitorQueries.sh
```


Clean the environment with the command

```
sudo make down
```

## Interface

* Flink: http://localhost:8081

