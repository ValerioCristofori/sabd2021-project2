build:
	mvn clean package assembly:single
up:
	./docker/start-docker.sh
down:
	./docker/stop-docker.sh
