.PHONY: all indexer reader backends release

PWD := $(shell pwd)

clean:
	mvn clean

build: clean
	mvn package

all: clean indexer reader report-coverage

report-coverage:
	docker run -it -v ~/.m2:/root/.m2 -w /src -v `pwd`:/src maven:3.5.0-jdk-8 /bin/sh -c 'mvn scoverage:report-only && mvn clean'

indexer: build_indexer
	#cd indexer && $(MAKE) integration_test

reader: build_reader
	#cd reader && $(MAKE) integration_test

build_reader:
	mvn package -DfinalName=haystack-trace-reader -pl reader -am

build_indexer:
	mvn package -DfinalName=haystack-trace-indexer -pl indexer -am

backends: cd storage_backends && (MAKE) all

# build all and release
release: clean build_indexer build_reader build_backends
	cd indexer && $(MAKE) docker_build && $(MAKE) release
	cd reader && $(MAKE) docker_build && $(MAKE) release
	cd storage_backends && (MAKE) release
	./.travis/deploy.sh

