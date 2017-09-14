# haystack-trace-reader

Service for fetching traces and fields from persistent storages.
 
##Technical Details

In order to understand this service, we recommend to read the details of [haystack](https://github.com/ExpediaDotCom/haystack) project. 
This service reads from [Cassandra](http://cassandra.apache.org/) and [ElasticSearch](https://www.elastic.co/) stores. API endpoints are exposed as [GRPC](https://grpc.io/) endpoints. 

Will fill in more details as we go..

## Building

####Prerequisite: 

* Make sure you have Java 1.8
* Make sure you have maven 3.3.9 or higher
* Make sure you have docker 1.13 or higher
* Make sure you have docker-compose 1.11 or higher

Note : For mac users you can download docker for mac to set you up for the last two steps.


####Build

For a full build, including unit tests, jar + docker image build and integration test, you can run -
```
make all
```

####Integration Test

If you are developing and just want to run integration tests 
```
make integration_test

```
