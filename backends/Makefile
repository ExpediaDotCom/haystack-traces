.PHONY: all cassandra release

PWD := $(shell pwd)

all: cassandra

cassandra: build_cassandra
	cd cassandra && $(MAKE) integration_test

build_cassandra:
	./../mvnw package -DfinalName=haystack-trace-backend-cassandra -pl cassandra -am

# release all backends
release:
	cd cassandra && $(MAKE) docker_build && $(MAKE) release
