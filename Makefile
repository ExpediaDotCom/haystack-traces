.PHONY: all indexer reader release

PWD := $(shell pwd)

clean:
	mvn clean

build: clean
	mvn package

all: clean indexer reader coverage

coverage:
	mvn scoverage:check scoverage:report 

indexer:
	mvn clean package -pl indexer -am
	cd indexer && $(MAKE) integration_test

reader:
	mvn clean package -pl reader -am
	cd reader && $(MAKE) integration_test

# build all and release
release: all
	cd indexer && $(MAKE) release
	cd reader && $(MAKE) release

