CREATE KEYSPACE IF NOT EXISTS {{keyspace}} WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 1} AND durable_writes = false;

use {{keyspace}};

CREATE TABLE {{table}} (
id varchar,
sid varchar,
span blob,
PRIMARY KEY ((id), sid)
) WITH CLUSTERING ORDER BY (sid ASC);

ALTER TABLE {{table}} WITH compaction = { 'class' :  'DateTieredCompactionStrategy'  };