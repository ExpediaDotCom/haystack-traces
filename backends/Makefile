.PHONY: all cassandra release

PWD := $(shell pwd)

clean:
	mvn clean

build: clean
	mvn package

all: clean cassandra report-coverage

report-coverage:
	docker run -it -v ~/.m2:/root/.m2 -w /src -v `pwd`:/src maven:3.5.0-jdk-8 /bin/sh -c 'mvn scoverage:report-only && mvn clean'

cassandra: build_cassandra
	cd cassandra && $(MAKE) integration_test

build_cassandra:
	mvn package mvn package -DfinalName=haystack-trace-backend-cassandra -pl cassandra -am -pl cassandra -am

# build all and release
release: clean build_cassandra
	cd cassandra && $(MAKE) docker_build && $(MAKE) release
	./.travis/deploy.sh

