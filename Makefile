.PHONY: all indexer reader release

PWD := $(shell pwd)

clean:
	mvn clean

build: clean
	mvn -Dgpg.skip package

all: clean indexer reader report-coverage

report-coverage:
	docker run -it -v ~/.m2:/root/.m2 -w /src -v `pwd`:/src maven:3.5.0-jdk-8 mvn scoverage:report-only

indexer: build_indexer
	cd indexer && $(MAKE) integration_test

reader: build_reader
	cd reader && $(MAKE) integration_test

build_reader:
	mvn -Dgpg.skip package -pl reader -am

build_indexer:
	mvn -Dgpg.skip package -pl indexer -am

# build all and release
release: all
	cd indexer && $(MAKE) release
	cd reader && $(MAKE) release
	./.travis/deploy.sh

