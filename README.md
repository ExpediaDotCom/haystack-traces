# haystack-span-stitcher
this haystack component stitches the spans for every unique traceId in a given time window(configurable). 
The time window for every unique traceId starts with the record timestamp of its first observed child span.

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