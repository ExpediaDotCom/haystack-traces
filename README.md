[![Build Status](https://travis-ci.org/ExpediaDotCom/haystack-traces.svg?branch=master)](https://travis-ci.org/ExpediaDotCom/haystack-traces)
[![License](https://img.shields.io/badge/license-Apache%20License%202.0-blue.svg)](https://github.com/ExpediaDotCom/haystack/blob/master/LICENSE)

# Haystack Traces

Traces is a subsystem included in Haystack that provides a distributed tracing system to troubleshoot problems in microservice architectures. Its design is based on the [Google Dapper](http://research.google.com/pubs/pub36356.html) paper.


This repo contains the haystack components that build the traces. It uses ElasticSearch for indexing and a storage backend for persistence

## Architecture
Please see the [architecture document](https://expediadotcom.github.io/haystack/docs/subsystems/subsystems_traces.html) for the high level architecture of the traces subsystem


## Components

### haystack-trace-indexer

Trace Indexer is the component which reads spans from a kafka topic and writes to elasticsearch(for indexing) 
and the storage backend for persistence. Please see the [indexer app](indexer/) for more details

### haystack-trace-reader

Trace Reader is the component which retrieves the trace-ids from elastic search based on the given queries and then fetches the spans from  
the storage backend. Please see the [reader app](reader/) for more details

### Storage Backend

Haystack Traces multiple storage backend apps, used to store and query spans. The Storage backend apps are 
grpc apps which are expected to implement this [grpc contract](https://github.com/ExpediaDotCom/haystack-idl/blob/master/proto/backend/storageBackend.proto)
The [reader](reader/src/main/scala/com/expedia/www/haystack/trace/reader/stores/readers/grpc/GrpcTraceReader.scala) and [indexer](indexer/src/main/scala/com/expedia/www/haystack/trace/indexer/writers/grpc/GrpcTraceWriter.scala) components read and write to the underlying datastore using this service and the default configuration expects the storage backend app to run on the same host(localhost) as the indexer and reader app.

By default the traces subsystem comes bundled with the following backends. You can always run your custom backends as long as it implements the [grpc contract](https://github.com/ExpediaDotCom/haystack-idl/blob/master/proto/backend/storageBackend.proto).

#### In-Memory
The in-memory storage backend app keeps the spans in memory. It
is neither persistent, nor viable for realistic work loads. Please see the [memory backend app](backends/memory) for more details


#### Cassandra
The Cassandra storage-backend app is tested against [Cassandra 3.11.3+](http://cassandra.apache.org/). It is designed for production scale. Please see the [cassandra backend app](backends/cassandra) for more details

#### Mysql
The Mysql storage-backend app is tested against [Mysql 5.6++](https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-13.html) and [amazon aurora mysql](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.AuroraMySQL.Overview.html).
It is designed for production scale. Please see the [mysql backend app](backends/mysql) for more details


## Contributing to this codebase
Please see [CONTRIBUTING.md](CONTRIBUTING.md)


## Bugs, Feature Requests, Documentation Updates
Please see the [contributing page](https://expediadotcom.github.io/haystack/docs/contributing.html) on our website

## Contact Info

Interested in haystack? Want to talk? Have questions, concerns or great ideas?
Please join us on [gitter](https://gitter.im/expedia-haystack/Lobby)

##License
By contributing to Haystack, you agree that your contributions will be licensed under its Apache License.