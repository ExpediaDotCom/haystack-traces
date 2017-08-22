/*
 *  Copyright 2017 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

const cassandra = require('cassandra-driver');

const client = new cassandra.Client({contactPoints: ['cassandra']});

client.execute("CREATE KEYSPACE IF NOT EXISTS haystack WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 1} AND durable_writes = false")
    .then(() => client.execute("USE haystack"))
    .then(() => client.execute("CREATE TABLE traces (id varchar, ts timestamp, stitchedspans blob, PRIMARY KEY ((id), ts)) WITH CLUSTERING ORDER BY (ts ASC)"))
    .then(() => client.execute("ALTER TABLE traces WITH compaction = { 'class' :  'DateTieredCompactionStrategy'  }"))
    .then(() => {
        client.shutdown();
        console.log("created traces table");
    });
