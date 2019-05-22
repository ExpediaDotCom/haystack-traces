.PHONY: all cassandra mysql memory release

PWD := $(shell pwd)

all: cassandra memory

cassandra: build_cassandra
	cd cassandra && $(MAKE) integration_test

build_cassandra:
	cd ../ && ./mvnw package -DfinalName=haystack-trace-backend-cassandra -pl backends/cassandra -am


mysql: build_mysql
	cd mysql && $(MAKE) integration_test

build_mysql:
	cd ../ && ./mvnw package -DfinalName=haystack-trace-backend-mysql -pl backends/mysql -am


memory: build_memory
	cd memory && $(MAKE) integration_test

build_memory:
	cd ../ && ./mvnw package -DfinalName=haystack-trace-backend-memory -pl backends/memory -am

# release all backends
release:
	cd cassandra && $(MAKE) docker_build && $(MAKE) release
	cd memory && $(MAKE) docker_build && $(MAKE) release
	cd mysql && $(MAKE) docker_build && $(MAKE) release
