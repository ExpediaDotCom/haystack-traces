/*
 *  Copyright 2017 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.expedia.www.haystack.trace.reader.stores.readers.cassandra

import com.expedia.open.tracing.api.Trace
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraSession
import com.expedia.www.haystack.trace.commons.config.entities.CassandraConfiguration
import com.expedia.www.haystack.trace.reader.metrics.{AppMetricNames, MetricsSupport}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}

class CassandraReader(cassandra: CassandraSession, config: CassandraConfiguration)(implicit val dispatcher: ExecutionContextExecutor)
  extends MetricsSupport with AutoCloseable {
  private val LOGGER = LoggerFactory.getLogger(classOf[CassandraReader])

  private val readTimer = metricRegistry.timer(AppMetricNames.CASSANDRA_READ_TIME)
  private val readFailures = metricRegistry.meter(AppMetricNames.CASSANDRA_READ_FAILURES)

  def readTrace(traceId: String): Future[Trace] = {
    val timer = readTimer.time()
    val promise = Promise[Trace]

    try {
      val statement = cassandra.newSelectTraceBoundStatement(traceId)
      val asyncResult = cassandra.executeAsync(statement)
      asyncResult.addListener(new CassandraReadTraceResultListener(asyncResult, timer, readFailures, promise), dispatcher)
      promise.future
    } catch {
      case ex: Exception =>
        readFailures.mark()
        timer.stop()
        LOGGER.error("Failed to read trace with exception", ex)
        Future.failed(ex)
    }
  }

  override def close(): Unit = ()
}
