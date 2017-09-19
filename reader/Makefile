.PHONY: prepare_integration_test_env integration_test release

export DOCKER_ORG := expediadotcom
export DOCKER_IMAGE_NAME := haystack-trace-reader
PWD := $(shell pwd)
SERVICE_DEBUG_ON ?= false

docker_build:
	# build docker image using existing app jar
	docker build -t $(DOCKER_IMAGE_NAME) -f build/docker/Dockerfile .

prepare_integration_test_env: docker_build
	# prepare environment to run integration tests against
	docker-compose -f build/integration-tests/docker-compose.yml -p sandbox up -d
	sleep 30

	# prepare cassandra table for haystack-trace-reader to run against
	docker run \
		-it \
		--network=sandbox_default \
		-v $(PWD)/build/integration-tests:/src \
		-w /src \
		node:alpine \
		/bin/sh -c 'npm install && node create-cassandra-table.js'

	# run haystack-trace-reader and join the same network of its dependent services
	docker run \
		-d \
		-p 5005:5005 \
		-p 8080:8080 \
		--name trace-reader \
		--network=sandbox_default \
		--network-alias=haystack-trace-reader \
		-v $(PWD)/build/integration-tests/docker-app.conf:/docker-app.conf \
		-e SERVICE_DEBUG_ON=$(SERVICE_DEBUG_ON) \
		-e OVERRIDES_CONFIG_PATH=/docker-app.conf \
		$(DOCKER_IMAGE_NAME)

integration_test: prepare_integration_test_env
	# run tests in a separate container and join the docker-compose network
	docker run \
		-it \
		--network=sandbox_default \
		-v $(PWD)/..:/src \
		-v ~/.m2:/root/.m2 \
		-w /src \
		maven:3.5.0-jdk-8 \
		mvn test -pl reader -am -P integration-tests

	# stop all the containers
	docker-compose -f build/integration-tests/docker-compose.yml -p sandbox stop
	docker stop trace-reader
	docker rm $(shell docker ps -a -q)
	docker volume rm $(shell docker volume ls -q)

release: docker_build
	../scripts/publish-to-docker-hub.sh