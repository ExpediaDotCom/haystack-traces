CREATE KEYSPACE IF NOT EXISTS {{keyspace}} WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 1} AND durable_writes = false;

use {{keyspace}};

CREATE TABLE {{table}} (
id varchar,
ts timestamp,
stitchedspans blob,
PRIMARY KEY ((id), ts)
) WITH CLUSTERING ORDER BY (ts ASC);

ALTER TABLE {{table}} WITH compaction = { 'class' :  'DateTieredCompactionStrategy'  };