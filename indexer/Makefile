.PHONY: docker_build integration_test release

export DOCKER_ORG := expediadotcom
export DOCKER_IMAGE_NAME := haystack-trace-indexer
PWD := $(shell pwd)

docker_build:
	docker build -t $(DOCKER_IMAGE_NAME) -f build/docker/Dockerfile .

prepare_integration_test_env: docker_build
	docker-compose -f build/integration-tests/docker-compose.yml -p sandbox up -d

	# cassandra takes time to start
	sleep 10

integration_test: prepare_integration_test_env
	# run tests in a container so that we can join the docker-compose network and talk to kafka, elasticsearch and cassandra
	docker run \
		-it \
		--rm \
		--network=sandbox_default \
		-v $(PWD)/..:/src \
		-v ~/.m2:/root/.m2 \
		-w /src \
		maven:3.5.0-jdk-8 \
		mvn -Dgpg.skip=true scoverage:integration-check -pl indexer -am

	# clean up the docker
	docker-compose -f build/integration-tests/docker-compose.yml -p sandbox stop
	docker rm $(shell docker ps -a -q)
	docker volume rm $(shell docker volume ls -q)

release:
	../scripts/publish-to-docker-hub.sh
