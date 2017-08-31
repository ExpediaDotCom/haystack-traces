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

package com.expedia.www.haystack.trace.indexer.writers.cassandra

import java.nio.ByteBuffer
import java.util.Date

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.expedia.www.haystack.trace.indexer.config.entities.CassandraConfiguration
import com.expedia.www.haystack.trace.indexer.metrics.{AppMetricNames, MetricsSupport}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.Try

class CassandraWriter(config: CassandraConfiguration)(implicit val dispatcher: ExecutionContextExecutor)
  extends MetricsSupport with AutoCloseable {

  private val LOGGER = LoggerFactory.getLogger(classOf[CassandraWriter])

  private val writeTimer = metricRegistry.timer(AppMetricNames.CASSANDRA_WRITE_TIME)
  private val writeFailures = metricRegistry.meter(AppMetricNames.CASSANDRA_WRITE_FAILURE)
  private val sessionFactory = new CassandraSessionFactory(config)

  //insert into table(id, ts, span) values (?, ?, ?) using ttl ?
  private lazy val insertSpan: PreparedStatement = {
    import QueryBuilder.{bindMarker, ttl}
    import Schema._

    sessionFactory.session.prepare(
      QueryBuilder
        .insertInto(config.tableName)
        .value(ID_COLUMN_NAME, bindMarker(ID_COLUMN_NAME))
        .value(TIMESTAMP_COLUMN_NAME, bindMarker(TIMESTAMP_COLUMN_NAME))
        .value(SPANS_COLUMN_NAME, bindMarker(SPANS_COLUMN_NAME))
        .using(ttl(config.recordTTLInSec)))
  }

  /**
    * writes the traceId and its spans to cassandra. Use the current timestamp as the sort key for the writes to same
    * TraceId
    * @param traceId: trace id
    * @param spanBufferBytes: list of spans belonging to this traceId
    * @return
    */
  def write(traceId: String, spanBufferBytes: Array[Byte]): Future[_] = {
    try {
      val timer = writeTimer.time()
      val promise = Promise[Boolean]()
      val batchStatement = prepareBatchStatement(traceId, spanBufferBytes)
      val asyncResult = sessionFactory.session.executeAsync(batchStatement)
      asyncResult.addListener(new CassandraWriteResultListener(asyncResult, timer, promise), dispatcher)
      promise.future
    } catch {
      case ex: Exception =>
        LOGGER.error("Fail to write the spans to cassandra with reason", ex)
        writeFailures.mark()
        Future.failed(ex)
    }
  }

  private def prepareBatchStatement(traceId: String, spansByte: Array[Byte]): Statement = {
    new BoundStatement(insertSpan)
      .setString(Schema.ID_COLUMN_NAME, traceId)
      .setTimestamp(Schema.TIMESTAMP_COLUMN_NAME, new Date())
      .setBytes(Schema.SPANS_COLUMN_NAME, ByteBuffer.wrap(spansByte))
      .setConsistencyLevel(config.consistencyLevel)
  }

  override def close(): Unit = {
    LOGGER.info("Closing cassandra session now..")
    Try(sessionFactory.close())
  }
}
