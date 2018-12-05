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

package com.expedia.www.haystack.trace.storage.backends.cassandra.store

import java.util.concurrent.Semaphore

import com.expedia.open.tracing.backend.TraceRecord
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.commons.retries.RetryOperation._
import com.expedia.www.haystack.trace.storage.backends.cassandra.client.CassandraSession
import com.expedia.www.haystack.trace.storage.backends.cassandra.config.entities.CassandraWriteConfiguration
import com.expedia.www.haystack.trace.storage.backends.cassandra.metrics.AppMetricNames
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

class CassandraTraceRecordsWriter(cassandra: CassandraSession,
                                  config: CassandraWriteConfiguration)(implicit val dispatcher: ExecutionContextExecutor)
  extends MetricsSupport {


  private val LOGGER = LoggerFactory.getLogger(classOf[CassandraTraceRecordsWriter])

  cassandra.ensureKeyspace(config.clientConfig.tracesKeyspace)

  private val writeTimer = metricRegistry.timer(AppMetricNames.CASSANDRA_WRITE_TIME)
  private val writeFailures = metricRegistry.meter(AppMetricNames.CASSANDRA_WRITE_FAILURE)

  private val spanInsertPreparedStmt = cassandra.createSpanInsertPreparedStatement(config.clientConfig.tracesKeyspace)

  // this semaphore controls the parallel writes to cassandra
  private val inflightRequestsSemaphore = new Semaphore(config.maxInFlightRequests, true)


  private def execute(record: TraceRecord): Unit = {
    // execute the request async with retry
    withRetryBackoff(retryCallback => {
      val timer = writeTimer.time()

      // prepare the statement
      val statement = cassandra.newTraceInsertBoundStatement(record.getTraceId,
        record.getSpans.toByteArray,
        config.writeConsistencyLevel(retryCallback.lastError()),
        spanInsertPreparedStmt)

      val asyncResult = cassandra.executeAsync(statement)
      asyncResult.addListener(new CassandraTraceRecordsWriteResultListener(asyncResult, timer, retryCallback), dispatcher)
    },
      config.retryConfig,
      onSuccess = (_: Any) => inflightRequestsSemaphore.release(),
      onFailure = ex => {
        inflightRequestsSemaphore.release()
        LOGGER.error(s"Fail to write to cassandra after ${config.retryConfig.maxRetries} retry attempts for ${record.getTraceId}", ex)
      })
  }

  /**
    * writes the traceId and its spans to cassandra. Use the current timestamp as the sort key for the writes to same
    * TraceId. Also if the parallel writes exceed the max inflight requests, then we block and this puts backpressure on
    * upstream
    *
    * @param traceRecords          : trace records which need to be written
    * @return
    */
  def writeTraceRecords(traceRecords: List[TraceRecord]): Unit = {
    var isSemaphoreAcquired = false

    traceRecords.foreach(record => {

      try {
        inflightRequestsSemaphore.acquire()
        isSemaphoreAcquired = true
        /* write spanBuffer for a given traceId */
        execute(record)
      } catch {
        case ex: Exception =>
          LOGGER.error("Fail to write the spans to cassandra with exception", ex)
          writeFailures.mark()
          if (isSemaphoreAcquired) inflightRequestsSemaphore.release()
      }
    })
  }

  def close(): Unit = {
    LOGGER.info("Closing cassandra session now..")
    Try(cassandra.close())
  }
}
