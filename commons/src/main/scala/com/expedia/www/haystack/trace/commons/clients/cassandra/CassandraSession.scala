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

package com.expedia.www.haystack.trace.commons.clients.cassandra

import java.nio.ByteBuffer
import java.util.Date

import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraTableSchema._
import com.expedia.www.haystack.trace.commons.config.entities.{CassandraConfiguration, KeyspaceConfiguration}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try

class CassandraSession(config: CassandraConfiguration, factory: ClusterFactory) {
  private val LOGGER = LoggerFactory.getLogger(classOf[CassandraSession])

  /**
    * builds a session object to interact with cassandra cluster
    * Also ensure that keyspace and table names exists in cassandra.
    */
  private val (cluster, session) = {
    val cluster = factory.buildCluster(config)
    val newSession = cluster.connect()
    (cluster, newSession)
  }

  def ensureKeyspace(keyspace: KeyspaceConfiguration): Unit = {
    LOGGER.info("ensuring kespace exists with {}", keyspace)
    CassandraTableSchema.ensureExists(keyspace.name, keyspace.table, keyspace.autoCreateSchema, session)
  }

  private lazy val selectTracePreparedStmt: PreparedStatement = {
    import QueryBuilder.bindMarker
    session.prepare(
      QueryBuilder
        .select()
        .from(config.tracesKeyspace.name, config.tracesKeyspace.table)
        .where(QueryBuilder.eq(ID_COLUMN_NAME, bindMarker(ID_COLUMN_NAME))))
  }

  lazy val selectRawTracesPreparedStmt: PreparedStatement = {
    import QueryBuilder.bindMarker
    session.prepare(
      QueryBuilder
        .select()
        .from(config.tracesKeyspace.name, config.tracesKeyspace.table)
        .where(QueryBuilder.in(ID_COLUMN_NAME, bindMarker(ID_COLUMN_NAME))))
  }

  def createServiceMetadataInsertPreparedStatement(keyspace: KeyspaceConfiguration): PreparedStatement = {
    import QueryBuilder.{bindMarker, ttl}

    val insert = QueryBuilder
      .insertInto(keyspace.name, keyspace.table)
      .value(SERVICE_COLUMN_NAME, bindMarker(SERVICE_COLUMN_NAME))
      .value(OPERATION_COLUMN_NAME, bindMarker(OPERATION_COLUMN_NAME))
      .value(TIMESTAMP_COLUMN_NAME, bindMarker(TIMESTAMP_COLUMN_NAME))
      .using(ttl(keyspace.recordTTLInSec))

    session.prepare(insert)
  }

  def createSpanInsertPreparedStatement(keyspace: KeyspaceConfiguration): PreparedStatement = {
    import QueryBuilder.{bindMarker, ttl}

    val insert = QueryBuilder
      .insertInto(keyspace.name, keyspace.table)
      .value(ID_COLUMN_NAME, bindMarker(ID_COLUMN_NAME))
      .value(TIMESTAMP_COLUMN_NAME, bindMarker(TIMESTAMP_COLUMN_NAME))
      .value(SPANS_COLUMN_NAME, bindMarker(SPANS_COLUMN_NAME))
      .using(ttl(keyspace.recordTTLInSec))

    session.prepare(insert)
  }

  /**
    * close the session and client
    */
  def close(): Unit = {
    Try(session.close())
    Try(cluster.close())
  }

  /**
    * create bound statement for writing to cassandra table
    *
    * @param serviceName                    name of service ie primary key in cassandra
    * @param operationList                  name of operation
    * @param consistencyLevel               consistency level for cassandra write
    * @param insertServiceMetadataStatement prepared statement to use
    * @return
    */
  def newServiceMetadataInsertStatement(serviceName: String,
                                        operationList: Iterable[String],
                                        consistencyLevel: ConsistencyLevel,
                                        insertServiceMetadataStatement: PreparedStatement): Statement = {

    val statements = operationList.map(operation => {
      new BoundStatement(insertServiceMetadataStatement)
        .setString(SERVICE_COLUMN_NAME, serviceName)
        .setString(OPERATION_COLUMN_NAME, operation)
        .setTimestamp(TIMESTAMP_COLUMN_NAME, new Date())
        .setConsistencyLevel(consistencyLevel)
    })

    new BatchStatement(Type.UNLOGGED).addAll(statements.asJava)
  }

  /**
    * create bound statement for writing to cassandra table
    *
    * @param traceId              trace id
    * @param spanBufferBytes      data bytes of spanBuffer that belong to a given trace id
    * @param consistencyLevel     consistency level for cassandra write
    * @param insertTraceStatement prepared statement to use
    * @return
    */
  def newTraceInsertBoundStatement(traceId: String,
                                   spanBufferBytes: Array[Byte],
                                   consistencyLevel: ConsistencyLevel,
                                   insertTraceStatement: PreparedStatement): Statement = {
    new BoundStatement(insertTraceStatement)
      .setString(ID_COLUMN_NAME, traceId)
      .setTimestamp(TIMESTAMP_COLUMN_NAME, new Date())
      .setBytes(SPANS_COLUMN_NAME, ByteBuffer.wrap(spanBufferBytes))
      .setConsistencyLevel(consistencyLevel)
  }

  /**
    * create new select statement for retrieving data for traceId
    *
    * @param traceId trace id
    * @return
    */
  def newSelectTraceBoundStatement(traceId: String): Statement = {
    new BoundStatement(selectTracePreparedStmt).setString(ID_COLUMN_NAME, traceId)
  }

  /**
    * create new select statement for retrieving Raw Traces data for traceIds
    *
    * @param traceIds list of trace id
    * @return statement for select query for traceIds
    */
  def newSelectRawTracesBoundStatement(traceIds: List[String]): Statement = {
    new BoundStatement(selectRawTracesPreparedStmt).setList(ID_COLUMN_NAME, traceIds.asJava)
  }

  /**
    * executes the statement async and return the resultset future
    *
    * @param statement prepared statement to be executed
    * @return future object of ResultSet
    */
  def executeAsync(statement: Statement): ResultSetFuture = session.executeAsync(statement)

  def newSelectServicePreparedStatement(keyspace: String, table: String): PreparedStatement = {
    import QueryBuilder.bindMarker

    val select = QueryBuilder
      .select(OPERATION_COLUMN_NAME)
      .from(keyspace, table)
      .limit(1000)
      .where(QueryBuilder.eq(SERVICE_COLUMN_NAME, bindMarker(SERVICE_COLUMN_NAME)))
    session.prepare(select)
  }

  def newSelectAllServicesPreparedStatement(keyspace: String, table: String): PreparedStatement = {
    session.prepare(QueryBuilder.select(SERVICE_COLUMN_NAME).distinct().from(keyspace, table))
  }
}
