# haystack-span-stitcher
this haystack component stitches the spans for every unique traceId in a given time window(configurable). 
The time window for every unique traceId starts with the minimum timestamp carried by its child spans.

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

####Debugging

1. Open Makefile
2. Add the following arguments to docker run step in `run_integration_test` trigger :
   ```-e SERVICE_DEBUG_ON=true -P 5005:5005```
3. fire `create_integration_test_env`
4. attach a remote debugger on port 5005
5. attach the breakpoints and run the integration test `make run_integration_test` to push data to local kafka container and start debuggging. 


##The compose file

Docker compose is a tool provided by docker which lets you define multiple services in a single file([docker-compose.yml](https://docs.docker.com/compose/compose-file/)).
All the docker services listed in the compose file and started on the local box when we fire the docker-compose up command. 

We are using docker-compose for our local development and testing since we don't want to have a dependency on a remote kinesis and kafka deployment for validating the functionality of this module
Here's the list of docker services we run to test the app locally
1. Kafka - where the app pushes the data
2. Zookeeper - needed by the kafka container to start up correctly
3. The haystack-span-stitcher app running as a docker-service