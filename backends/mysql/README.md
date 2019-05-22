# Storage Backend - Mysql

Grpc service which can read a write spans to a mysql cluster 

##Technical Details

In order to understand this service, we recommend to read the details of [haystack](https://github.com/ExpediaDotCom/haystack) project. 
This service reads from [Mysql](https://www.mysql.com/). API endpoints are exposed as [GRPC](https://grpc.io/) endpoints based on [this]((https://github.com/ExpediaDotCom/haystack-idl/blob/master/proto/backend/storageBackend.proto)) contract. 

The Schema for the sql table is created by the code when it starts up if it doesn't exist using the following command

`
CREATE DATABASE IF NOT EXISTS haystack; USE haystack; create table IF NOT EXISTS spans (id  varchar(255) not null, spans LONGBLOB not null, ts timestamp default CURRENT_TIMESTAMP, PRIMARY KEY (id, ts))
`

## Deployments
The reader and the indexer app expects the storage-backend app as a sidecar container and sample deployment topology using docker compose is shared [here](docker-compose.yml)


## Building
Check the details on [Build Section](../../CONTRIBUTING.md)