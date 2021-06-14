build:
	mvn clean package
up:
	docker-compose up -d
down:
	docker-compose down
app:
	docker-compose build
	docker-compose up