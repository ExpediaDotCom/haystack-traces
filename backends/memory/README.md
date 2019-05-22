# Storage Backend - In Memory

Grpc service which can read a write spans to a an in memory map

##Technical Details

In order to understand this service, we recommend to read the details of [haystack](https://github.com/ExpediaDotCom/haystack) project. 
This service reads from an in memory map. API endpoints are exposed as [GRPC](https://grpc.io/) endpoints based on [this]((https://github.com/ExpediaDotCom/haystack-idl/blob/master/proto/backend/storageBackend.proto)) contract. 

* Note : Its purpose is for testing, for example starting a server on your laptop without any database needed. This only works if the reader and indexer apps are running locally and talk to the same in-memory backend server.

# Building
Check the details on [Build Section](../../CONTRIBUTING.md)