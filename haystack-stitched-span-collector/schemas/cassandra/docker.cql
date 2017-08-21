CREATE KEYSPACE IF NOT EXISTS haystack WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 1} AND durable_writes = false;

use haystack;

CREATE TABLE traces (
id varchar,
ts timestamp,
stitchedspans blob,
PRIMARY KEY ((id), ts)
) WITH CLUSTERING ORDER BY (ts ASC);

ALTER TABLE traces WITH compaction = { 'class' :  'DateTieredCompactionStrategy'  };