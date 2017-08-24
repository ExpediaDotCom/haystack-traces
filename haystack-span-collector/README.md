# haystack-span-collector
this haystack component collects the span buffers(complete or partial) for every traceId and write it 
to cassandra(as main store for spans) and elastic search(for indexing).

##Required Reading

In order to understand the haystack, we recommend to read the details of [haystack](https://github.com/ExpediaDotCom/haystack) project.
Its written in kafka-streams(http://docs.confluent.io/current/streams/index.html) and hence some prior knowledge of kafka-streams would be useful.


##Technical Details
Fill this as we go along..

## Building

####Prerequisite:

* Make sure you have Java 1.8
* Make sure you have maven 3.3.9 or higher
* Make sure you have docker 1.13 or higher


Note : For mac users you can download docker for mac to set you up for the last two steps.


####Build

For a full build, including unit tests and integration tests, docker image build, you can run -
```
make all
```

####Integration Test

####Prerequisite:
1. Install docker using Docker Tools or native docker if on mac
2. Verify if docker-compose is installed by running following command else install it.
```
docker-compose

```

Run the inegration tests with
```
make integration_test

```
