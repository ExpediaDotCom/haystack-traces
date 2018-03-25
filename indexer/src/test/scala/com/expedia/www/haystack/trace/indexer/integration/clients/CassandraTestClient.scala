/*
 *  Copyright 2017 Expedia, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.expedia.www.haystack.trace.indexer.integration.clients

import com.datastax.driver.core.{Cluster, ConsistencyLevel, SimpleStatement}
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.commons.retries.RetryOperation
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraTableSchema
import com.expedia.www.haystack.trace.commons.config.entities.{CassandraConfiguration, SocketConfiguration}
import com.expedia.www.haystack.trace.indexer.config.entities.CassandraWriteConfiguration

import scala.collection.JavaConversions._

class CassandraTestClient {
  case class CassandraRow(id: String, timestamp: java.util.Date, spanBuffer: SpanBuffer)

  private val CASSANDRA_ENDPOINT = "cassandra"
  private val KEYSPACE = "haystack"
  private val TABLE_NAME = "traces"
  private val cassandraSession = Cluster.builder().addContactPoints(CASSANDRA_ENDPOINT).build().connect()

  private def truncateDataIfPresent(): Unit = {
    cassandraSession.execute(new SimpleStatement(s"TRUNCATE $KEYSPACE.$TABLE_NAME"))
  }

  def prepare(): Unit = {
    CassandraTableSchema.ensureExists(
      KEYSPACE,
      TABLE_NAME,
      Some("CREATE KEYSPACE IF NOT EXISTS haystack WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 1} AND durable_writes = false;\n\nuse haystack;\n\nCREATE TABLE traces (\nid varchar,\nts timestamp,\nspans blob,\nPRIMARY KEY ((id), ts)\n) WITH CLUSTERING ORDER BY (ts ASC);\n\nALTER TABLE traces WITH compaction = { 'class' :  'DateTieredCompactionStrategy'  };"),
      cassandraSession)
    truncateDataIfPresent()
  }

  def buildConfig = CassandraWriteConfiguration(
    CassandraConfiguration(List(CASSANDRA_ENDPOINT),
      autoDiscoverEnabled = false,
      None,
      None,
      KEYSPACE,
      TABLE_NAME,
      None,
      SocketConfiguration(5, keepAlive = true, 5000, 5000)), ConsistencyLevel.ONE, 150, 10, RetryOperation.Config(10, 250, 2), List((Class.forName("com.datastax.driver.core.exceptions.UnavailableException"), ConsistencyLevel.ANY)))

  def queryAll(): Seq[CassandraRow] = {
    val rows = cassandraSession.execute(s"SELECT id, ts, spans from $KEYSPACE.$TABLE_NAME")
    val result = for (row <- rows)
      yield CassandraRow(row.getString("id"), row.getTimestamp("ts"), SpanBuffer.parseFrom(row.getBytes("spans").array()))
    result.toList
  }
}
