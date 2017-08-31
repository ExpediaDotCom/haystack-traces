# haystack-trace-indexer
This haystack component accumulates the spans associated with a TraceId in a given time window(configurable). 
The time window for every unique traceId starts with the kafka-record's timestamp of the first observed child span.
These accumulated spans are  written as single entity to external stores like cassandra and elastic search. We also output these
accumulated spans back to kafka for other consumers to consume.

The buffering approach provides a performance optimization as it will potentially reduce the number of write calls to external stores.
Secondly, the output can also by used by dependency graph component to build the complete call graph for all the services.

Note that the system can still emit partial spans for a given traceId, possible cases can be 
 * The time window is not configured correctly or doesn't match with the speed at which spans appear in kafka
 * On redeployment of this component, we might spit out partially buffered spans.
  
However, the partial buffered spans are ok to be written to cassandra and elastic search. In Cassandra, we use TraceId as the 
primary key and store buffered-spans as a time series with currentTimestamp as sort key.

In ElasticSearch, we use TraceId appended by a 4 character random ID with every document that we write. This ensures
that if the same TraceId reappears, we generate a new document.
 
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