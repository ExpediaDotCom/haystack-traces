.PHONY: all indexer reader release

PWD := $(shell pwd)

clean:
	mvn clean

build: clean
	mvn install package

all: clean indexer reader

indexer:
	mvn package -pl indexer -am
	cd indexer && $(MAKE) integration_test

reader:
	mvn package -pl reader -am
	cd reader && $(MAKE) integration_test

# build all and release
release: all
	cd indexer && $(MAKE) release
	cd reader && $(MAKE) release

