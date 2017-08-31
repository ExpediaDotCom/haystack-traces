.PHONY: all integration_test release

DOCKER_IMAGE_TAG := haystack-trace-indexer
PWD := $(shell pwd)
SERVICE_DEBUG_ON ?= false

clean:
	mvn clean

build:  clean
	mvn package

docker_build:
	docker build -t $(DOCKER_IMAGE_TAG) -f build/docker/Dockerfile .

prepare_integration_test_env: docker_build
	docker-compose -f build/integration-tests/docker-compose.yml -p sandbox up -d

	# cassandra takes time to start
	sleep 45

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
REPO := lib/haystack-trace-indexer
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
ifeq ($(BRANCH), master)
release: all
	docker tag $(DOCKER_IMAGE_TAG) $(REPO):latest
	docker push $(REPO)
else
release: all
endif
