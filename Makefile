build:
	mvn clean package
up:
	./docker/start-proj.sh
down:
	./docker/stop-proj.sh
