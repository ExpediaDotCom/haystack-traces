.PHONY: all indexer reader release

PWD := $(shell pwd)

clean:
	mvn clean

build: clean
	mvn package

all: clean indexer reader report-coverage

report-coverage:
	docker run -it -v ~/.m2:/root/.m2 -w /src -v `pwd`:/src maven:3.5.0-jdk-8 /bin/sh -c 'mvn scoverage:report-only && mvn clean'

indexer: build_indexer
	cd indexer && $(MAKE) integration_test

reader: build_reader
	cd reader && $(MAKE) integration_test

build_reader:
	mvn package -pl reader -am

build_indexer:
	mvn package -pl indexer -am

# build all and release
release: all
	./.travis/deploy.sh

