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

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraTableSchema._
import com.expedia.www.haystack.trace.commons.config.entities.CassandraConfiguration

import scala.util.Try

class CassandraSession(config: CassandraConfiguration, factory: ClusterFactory) {
  /**
    * builds a session object to interact with cassandra cluster
    * Also ensure that keyspace and table names exists in cassandra.
    */
  private val (cluster, session) = {
    val cluster = factory.buildCluster(config)
    val newSession = cluster.connect()
    CassandraTableSchema.ensureExists(config.keyspace, config.tableName, config.autoCreateSchema, newSession)
    newSession.execute("USE " + config.keyspace)
    (cluster, newSession)
  }

  private val selectTracePreparedStmt: PreparedStatement = {
      import QueryBuilder.bindMarker
      session.prepare(
        QueryBuilder
          .select()
          .from(config.tableName)
          .where(QueryBuilder.eq(ID_COLUMN_NAME, bindMarker(ID_COLUMN_NAME))))
  }

  def createInsertPreparedStatement(recordTTLInSec: Int): PreparedStatement = {
    import QueryBuilder.{bindMarker, ttl}

    val insert = QueryBuilder
      .insertInto(config.tableName)
      .value(ID_COLUMN_NAME, bindMarker(ID_COLUMN_NAME))
      .value(TIMESTAMP_COLUMN_NAME, bindMarker(TIMESTAMP_COLUMN_NAME))
      .value(SPANS_COLUMN_NAME, bindMarker(SPANS_COLUMN_NAME))
      .using(ttl(recordTTLInSec))

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
    * @param traceId trace id
    * @param spanBufferBytes data bytes of spanBuffer that belong to a given trace id
    * @return
    */
  def newInsertBoundStatement(traceId: String,
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
    * @param traceId trace id
    * @return
    */
  def newSelectBoundStatement(traceId: String): Statement = {
    new BoundStatement(selectTracePreparedStmt).setString(ID_COLUMN_NAME, traceId)
  }

  /**
    * executes the statement async and return the resultset future
    * @param statement prepared statement to be executed
    * @return future object of ResultSet
    */
  def executeAsync(statement: Statement): ResultSetFuture = session.executeAsync(statement)
}
