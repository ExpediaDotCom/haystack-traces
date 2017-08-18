.PHONY: all prepare_integration_test_env integration_test release

DOCKER_IMAGE_TAG := haystack-stitch-span-collector
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
	sleep 15

	# run haystack-stitch-span-collector and join the same network of its dependent services
	docker run \
		-d \
		-p 5005:5005 \
		--network=sandbox_default \
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
		maven:alpine \
		mvn test -P integration-tests

	# stop allthe containers
	docker-compose -f build/integration-tests/docker-compose.yml -p sandbox stop

all: build docker_build integration_test

# build all and release
REPO := lib/haystack-stitch-span-collector
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
ifeq ($(BRANCH), master)
release: all
	docker tag $(DOCKER_IMAGE_TAG) $(REPO):latest
	docker push $(REPO)
else
release: all
endif
