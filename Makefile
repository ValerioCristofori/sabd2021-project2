build:
	mvn clean install package
	cp target/*-jar-with-dependencies.jar dist/.
up:
	docker-compose up -d
down:
	docker-compose down