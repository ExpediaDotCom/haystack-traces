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
`import com.expedia.www.haystack.trace.commons.config.entities.{CassandraConfiguration, KeyspaceConfiguration, SocketConfiguration}
import com.expedia.www.haystack.trace.indexer.config.entities.{CassandraWriteConfiguration, ServiceMetadataWriteConfiguration}

import scala.collection.JavaConverters._

class CassandraTestClient {

  case class CassandraSpanRow(id: String, timestamp: java.util.Date, spanBuffer: SpanBuffer)
  case class ServiceMetadataRow(serviceName: String, operationName: String, timestamp: java.util.Date)
  private val CASSANDRA_ENDPOINT = "cassandra"
  private val KEYSPACE = "haystack"
  private val TABLE_NAME = "traces"

  private val SERVICES_METADATA_KEYSPACE = "haystack_metadata"
  private val SERVICES_TABLE_NAME = "services"
  private val cassandraSession = Cluster.builder().addContactPoints(CASSANDRA_ENDPOINT).build().connect()
  private val traceSchema = Some("CREATE KEYSPACE IF NOT EXISTS haystack WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 1} AND durable_writes = false;\n\nCREATE TABLE haystack.traces (\nid varchar,\nts timestamp,\nspans blob,\nPRIMARY KEY ((id), ts)\n) WITH CLUSTERING ORDER BY (ts ASC);\n\nALTER TABLE haystack.traces WITH compaction = { 'class' :  'DateTieredCompactionStrategy'  };")
  private val metadataSchema = Some("CREATE KEYSPACE IF NOT EXISTS haystack_metadata WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 1} AND durable_writes = false;\n\nCREATE TABLE haystack_metadata.services (\nservice_name varchar,\noperation_name varchar,\nts timestamp,\nPRIMARY KEY ((service_name), operation_name)\n) WITH CLUSTERING ORDER BY (operation_name ASC);\n\nALTER TABLE haystack_metadata.services WITH compaction = { 'class' :  'DateTieredCompactionStrategy'  };")

  def prepare(): Unit = {
    cassandraSession.execute(new SimpleStatement(s"DROP KEYSPACE  $KEYSPACE"))
    cassandraSession.execute(new SimpleStatement(s"DROP KEYSPACE $SERVICES_METADATA_KEYSPACE"))
  }

  def buildServiceMetadataConfig: ServiceMetadataWriteConfiguration = {
    ServiceMetadataWriteConfiguration(enabled = true,
      10,
      1,
      1000,
      RetryOperation.Config(10, 250, 2),
      ConsistencyLevel.ONE,
      KeyspaceConfiguration(SERVICES_METADATA_KEYSPACE, SERVICES_TABLE_NAME, 10000, metadataSchema))
  }

  def buildConfig = CassandraWriteConfiguration(
    CassandraConfiguration(List(CASSANDRA_ENDPOINT),
      autoDiscoverEnabled = false,
      None,
      None,
      KeyspaceConfiguration(KEYSPACE, TABLE_NAME, 10000, traceSchema),
      SocketConfiguration(5, keepAlive = true, 5000, 5000)), ConsistencyLevel.ONE, 10, RetryOperation.Config(10, 250, 2), List((Class.forName("com.datastax.driver.core.exceptions.UnavailableException"), ConsistencyLevel.ANY)))

  def queryAllTraces(unpack: (Array[Byte] => SpanBuffer)): Seq[CassandraSpanRow] = {
    val rows = cassandraSession.execute(s"SELECT id, ts, spans from $KEYSPACE.$TABLE_NAME")
    val result = for (row <- rows.asScala)
      yield CassandraSpanRow(row.getString("id"), row.getTimestamp("ts"), unpack(row.getBytes("spans").array()))
    result.toSeq
  }

  def queryServices(): Seq[ServiceMetadataRow] = {
    val rows = cassandraSession.execute(s"SELECT service_name, operation_name, ts from $SERVICES_METADATA_KEYSPACE.$SERVICES_TABLE_NAME")
    val result = for (row <- rows.asScala)
      yield ServiceMetadataRow(row.getString("service_name"), row.getString("operation_name"), row.getTimestamp("ts"))
    result.toSeq
  }
}
