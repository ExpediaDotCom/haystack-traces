.PHONY: all clean build prepare_integration_test_env integration_test release

DOCKER_IMAGE_TAG := haystack-trace-provider
PWD := $(shell pwd)
SERVICE_DEBUG_ON ?= false

clean:
	mvn clean

build: clean
	# build jar
	mvn package

docker_build:
	# build docker image using existing app jar
	docker build -t $(DOCKER_IMAGE_TAG) -f build/docker/Dockerfile .

prepare_integration_test_env: docker_build
	# prepare environment to run integration tests against
	docker-compose -f build/integration-tests/docker-compose.yml -p sandbox up -d
	sleep 30

	# prepare cassandra table for haystack-trace-provider to run against
	docker run \
		-it \
		--network=sandbox_default \
		-v $(PWD)/build/integration-tests:/src \
		-w /src \
		node:alpine \
		/bin/sh -c 'npm install && node create-cassandra-table.js'

	# run haystack-trace-provider and join the same network of its dependent services
	docker run \
		-d \
		-p 5005:5005 \
		-p 8080:8080 \
		--network=sandbox_default \
		--network-alias=haystack-trace-provider \
		-v $(PWD)/build/integration-tests/docker-app.conf:/docker-app.conf \
		-e SERVICE_DEBUG_ON=$(SERVICE_DEBUG_ON) \
		-e OVERRIDES_CONFIG_PATH=/docker-app.conf \
		$(DOCKER_IMAGE_TAG)

integration_test: prepare_integration_test_env
	# run tests in a separate container and join the docker-compose network
	docker run \
		-it \
		--network=sandbox_default \
		-v $(PWD):/src \
		-v ~/.m2:/root/.m2 \
		-w /src \
		maven:3.5.0-jdk-8 \
		mvn test -P integration-tests

	# stop all the containers
	docker-compose -f build/integration-tests/docker-compose.yml -p sandbox stop

# build jar, docker image and run integration tests
all: build docker_build integration_test

# build all and release
REPO := lib/haystack-trace-provider
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
ifeq ($(BRANCH), master)
release: all
	docker tag $(DOCKER_IMAGE_TAG) $(REPO):latest
	docker push $(REPO)
else
release: all
endif
