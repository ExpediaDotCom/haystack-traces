.PHONY: all cassandra memory release

PWD := $(shell pwd)

all: cassandra

cassandra: build_cassandra
	cd cassandra && $(MAKE) integration_test

build_cassandra:
	cd ../ && ./mvnw clean package -DfinalName=haystack-trace-backend-cassandra -pl backends/cassandra -am


memory: build_memory
	cd memory && $(MAKE) integration_test

build_memory:
	cd ../ && ./mvnw clean package -DfinalName=haystack-trace-backend-memory -pl backends/memory -am

# release all backends
release:
	cd cassandra && $(MAKE) docker_build && $(MAKE) release
