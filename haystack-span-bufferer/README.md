# haystack-span-bufferer
This haystack component buffers the spans for every unique traceId for a given time window(configurable). 
The time window for every unique traceId starts with the record timestamp of its first observed child span.

This span buffering approach may provide following advantages: 

* It provides a performance optimization to buffered-span collector that writes these as single document to external stores like Cassandra and ElasticSearch.
We can potentially reduce the number of write operations to these external stores.

* This can also by used by dependency graph component that uses the child spans to build the right call graph for all the services.

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

If you are developing and just want to run integration tests 
```
make integration_test

```