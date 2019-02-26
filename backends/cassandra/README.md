# Storage Backend - Cassandra


Grpc service which can read a write spans to a cassandra cluster 

##Technical Details

In order to understand this service, we recommend to read the details of [haystack](https://github.com/ExpediaDotCom/haystack) project. 
This service reads from [Cassandra](http://cassandra.apache.org/). API endpoints are exposed as [GRPC](https://grpc.io/) endpoints based on [this]((https://github.com/ExpediaDotCom/haystack-idl/blob/master/proto/backend/storageBackend.proto)) contract. 

The Schema for the cassandra table is created by the code when it starts up if it doesn't exist using the following command

`
CREATE KEYSPACE IF NOT EXISTS haystack WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes = false; CREATE TABLE IF NOT EXISTS haystack.traces (id varchar, ts timestamp, spans blob, PRIMARY KEY ((id), ts)) WITH CLUSTERING ORDER BY (ts ASC) AND compaction = { 'class' :  'DateTieredCompactionStrategy', 'max_sstable_age_days': '3' } AND gc_grace_seconds = 86400;
`

## Deployments
The reader and the indexer app expects the storage-backend app as a sidecar container and sample deployment topology using docker compose is shared [here](https://github.com/ExpediaDotCom/haystack-docker)


## Building
Check the details on [Build Section](../../CONTRIBUTING.md)