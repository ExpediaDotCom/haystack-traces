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
import com.datastax.driver.core.policies._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.commons.clients.AwsNodeDiscoverer
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraTableSchema._
import com.expedia.www.haystack.trace.commons.config.entities.CassandraConfiguration

import scala.util.Try

class CassandraSession(config: CassandraConfiguration) {
  var cluster: Cluster = _

  lazy val selectTracePreparedStmt: PreparedStatement = {
      import QueryBuilder.bindMarker
      session.prepare(
        QueryBuilder
          .select()
          .from(config.tableName)
          .where(QueryBuilder.eq(ID_COLUMN_NAME, bindMarker(ID_COLUMN_NAME))))
  }

  def createInsertPreparedStatement(recordTTLInSec: Int): PreparedStatement = {
    import QueryBuilder.{bindMarker, ttl}

    session.prepare(
      QueryBuilder
        .insertInto(config.tableName)
        .value(ID_COLUMN_NAME, bindMarker(ID_COLUMN_NAME))
        .value(TIMESTAMP_COLUMN_NAME, bindMarker(TIMESTAMP_COLUMN_NAME))
        .value(SPANS_COLUMN_NAME, bindMarker(SPANS_COLUMN_NAME))
        .using(ttl(recordTTLInSec)))
  }

  /**
    * builds a session object to interact with cassandra cluster
    * Also ensure that keyspace and table names exists in cassandra.
    */
  val session: Session = {
    cluster = buildCluster()
    val newSession = cluster.connect()
    CassandraTableSchema.ensureExists(config.keyspace, config.tableName, config.autoCreateSchema, newSession)
    newSession.execute("USE " + config.keyspace)
    newSession
  }

  /**
    * close the session and client
    */
  def close(): Unit = {
    Try(session.close())
    Try(cluster.close())
  }

  private def discoverNodes(): Seq[String] = {
    config.awsNodeDiscovery match {
      case Some(awsDiscovery) => AwsNodeDiscoverer.discover(awsDiscovery.region, awsDiscovery.tags)
      case _ => Nil
    }
  }

  private def buildCluster(): Cluster = {
    val contactPoints = if(config.autoDiscoverEnabled) discoverNodes() else config.endpoints
    require(contactPoints.nonEmpty, "cassandra contact points can't be empty!!!")

    val tokenAwarePolicy = new TokenAwarePolicy(new LatencyAwarePolicy.Builder(new RoundRobinPolicy()).build())

    Cluster.builder()
      .withClusterName("cassandra-cluster")
      .addContactPoints(contactPoints:_*)
      .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
      .withSocketOptions(new SocketOptions()
        .setKeepAlive(config.socket.keepAlive)
        .setConnectTimeoutMillis(config.socket.connectionTimeoutMillis)
        .setReadTimeoutMillis(config.socket.readTimeoutMills))
      .withLoadBalancingPolicy(tokenAwarePolicy)
      .withPoolingOptions(new PoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, config.socket.maxConnectionPerHost))
      .build()
  }

  /**
    * create bound statement for writing to cassandra table
    * @param traceId trace id
    * @param spanBuffer array of span objects that belong to the given trace id
    * @return
    */
  def newInsertBoundStatement(traceId: String,
                              spanBuffer: SpanBuffer,
                              consistencyLevel: ConsistencyLevel,
                              insertTraceStatement: PreparedStatement): Statement = {

    new BoundStatement(insertTraceStatement)
      .setString(ID_COLUMN_NAME, traceId)
      .setTimestamp(TIMESTAMP_COLUMN_NAME, new Date())
      .setBytes(SPANS_COLUMN_NAME, ByteBuffer.wrap(spanBuffer.toByteArray))
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
}
