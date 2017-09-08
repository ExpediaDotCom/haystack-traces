.PHONY: all integration_test release

export DOCKER_ORG := expediadotcom
export DOCKER_IMAGE_NAME := haystack-trace-indexer
PWD := $(shell pwd)
SERVICE_DEBUG_ON ?= false

clean:
	mvn clean

build:  clean
	mvn package

docker_build:
	docker build -t $(DOCKER_IMAGE_NAME) -f build/docker/Dockerfile .

prepare_integration_test_env: docker_build
	docker-compose -f build/integration-tests/docker-compose.yml -p sandbox up -d

	# cassandra takes time to start
	sleep 30

integration_test: prepare_integration_test_env
	# run tests in a container so that we can join the docker-compose network and talk to kafka, elasticsearch and cassandra
	docker run \
		-it \
		--network=sandbox_default \
		-v $(PWD)/..:/src \
		-v ~/.m2:/root/.m2 \
		-w /src/indexer \
		maven:3.5.0-jdk-8 \
		mvn test -P integration-tests

	# stop all the containers
	docker-compose -f build/integration-tests/docker-compose.yml -p sandbox stop

all: build docker_build integration_test

# build all and release
release: all
	./build/docker/publish-to-docker-hub.sh
