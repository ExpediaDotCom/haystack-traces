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

package com.expedia.www.haystack.stitch.span.collector.writers.cassandra

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import com.codahale.metrics.Timer
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core._
import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.stitch.span.collector.config.entities.CassandraConfiguration
import com.expedia.www.haystack.stitch.span.collector.metrics.AppMetricNames
import com.expedia.www.haystack.stitch.span.collector.writers.StitchedSpanWriter
import com.expedia.www.haystack.stitch.span.collector.writers.cassandra.Schema._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}

class CassandraWriter(config: CassandraConfiguration)(implicit val dispatcher: ExecutionContextExecutor) extends StitchedSpanWriter {

  private val LOGGER = LoggerFactory.getLogger(classOf[CassandraWriter])

  private val writeTimer = metricRegistry.timer(AppMetricNames.CASSANDRA_WRITE_TIME)
  private val writeFailures = metricRegistry.meter(AppMetricNames.CASSANDRA_WRITE_FAILURE)
  private val writeWarnings = metricRegistry.meter(AppMetricNames.CASSANDRA_WRITE_WARNINGS)

  private val sessionFactory = new CassandraSessionFactory(config)

  private val errorLogCounter = new AtomicLong(0)

  //insert into table(id, sid, span) values (?, ?, ?)
  private lazy val insertSpan: PreparedStatement = sessionFactory.session.prepare(
    QueryBuilder
      .insertInto(config.tableName)
      .value(ID_COLUMN_NAME, QueryBuilder.bindMarker(ID_COLUMN_NAME))
      .value(SPAN_ID_COLUMN_NAME, QueryBuilder.bindMarker(SPAN_ID_COLUMN_NAME))
      .value(SPAN_COLUMN_NAME, QueryBuilder.bindMarker(SPAN_COLUMN_NAME)))

  private def prepareBatchStatement(stitchedSpan: StitchedSpan): BatchStatement = {
    val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)

    stitchedSpan.getChildSpansList.foreach(span => {
      val bound = new BoundStatement(insertSpan)
        .setString(ID_COLUMN_NAME, stitchedSpan.getTraceId)
        .setString(SPAN_ID_COLUMN_NAME, span.getSpanId)
        .setBytes(SPAN_COLUMN_NAME, ByteBuffer.wrap(span.toByteArray))
        .setConsistencyLevel(config.consistencyLevel)
      batch.add(bound)
    })

    batch
  }

  override def write(stitchedSpans: Seq[StitchedSpan]): Future[_] = {
    try {
      val futures: Seq[Future[Boolean]] = stitchedSpans.map { stitchedSpan =>
        val timer = writeTimer.time()
        val promise = Promise[Boolean]()
        val batchStatement = prepareBatchStatement(stitchedSpan)
        val asyncResult = sessionFactory.session.executeAsync(batchStatement)
        asyncResult.addListener(new CassandraResultListener(asyncResult, timer, promise), dispatcher)
        promise.future
      }
      Future.sequence(futures)
    } catch {
      case ex: Exception =>
        LOGGER.error("Fail to write the stitched spans to cassandra with reason", ex)
        writeFailures.mark()
        Future.failed(ex)
    }
  }

  override def close(): Unit = sessionFactory.close()

  private class CassandraResultListener(asyncResult: ResultSetFuture,
                                        timer: Timer.Context,
                                        promise: Promise[Boolean]) extends Runnable {
    override def run(): Unit = {
      try {
        timer.close()

        if (asyncResult.get != null &&
          asyncResult.get.getExecutionInfo != null &&
          asyncResult.get.getExecutionInfo.getWarnings != null &&
          asyncResult.get.getExecutionInfo.getWarnings.nonEmpty) {
          LOGGER.warn(s"Warning received in cassandra writes {}", asyncResult.get.getExecutionInfo.getWarnings.toList.mkString(","))
          writeWarnings.mark(asyncResult.get.getExecutionInfo.getWarnings.size())
        }
        promise.success(true)
      } catch {
        case ex: Exception =>
          if (errorLogCounter.incrementAndGet() % 100 == 0) {
            LOGGER.error("Fail to write the record to cassandra with exception", ex)
            errorLogCounter.set(0)
          }
          writeFailures.mark()
          promise.failure(ex)
      }
    }
  }
}
