/*
 *  Copyright 2018 Expedia, Inc.
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

package com.expedia.www.haystack.trace.indexer.writers

import java.time.Instant
import java.util.concurrent.Semaphore

import com.datastax.driver.core.Statement
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.commons.retries.RetryOperation.withRetryBackoff
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraSession
import com.expedia.www.haystack.trace.commons.packer.PackedMessage
import com.expedia.www.haystack.trace.indexer.config.entities.ServiceMetadataWriteConfiguration
import com.expedia.www.haystack.trace.indexer.metrics.AppMetricNames
import com.expedia.www.haystack.trace.indexer.writers.cassandra.{CassandraTraceWriter, CassandraWriteResultListener, ServiceMetadataStatementBuilder}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor

class ServiceMetadataWriter(cassandra: CassandraSession,
                            cfg: ServiceMetadataWriteConfiguration)
                           (implicit val dispatcher: ExecutionContextExecutor) extends TraceWriter with MetricsSupport {

  private val LOGGER = LoggerFactory.getLogger(classOf[CassandraTraceWriter])

  private val writeTimer = metricRegistry.timer(AppMetricNames.CASSANDRA_WRITE_TIME)
  private val writeFailures = metricRegistry.meter(AppMetricNames.CASSANDRA_WRITE_FAILURE)

  // this semaphore controls the parallel writes to cassandra
  private val inflightRequestsSemaphore = new Semaphore(cfg.maxInflight, true)

  private val statementBuilder = new ServiceMetadataStatementBuilder(cassandra, cfg.cassandraKeyspace, cfg.consistencyLevel)

  private var lastWriteInstant = Instant.MIN

  cassandra.ensureKeyspace(cfg.cassandraKeyspace)

  private def execute(statement: Statement) = {
    inflightRequestsSemaphore.acquire()

    withRetryBackoff((retryCallback) => {
      val timer = writeTimer.time()
      val asyncResult = cassandra.executeAsync(statement)
      asyncResult.addListener(new CassandraWriteResultListener(asyncResult, timer, retryCallback), dispatcher)
    },
      cfg.retryConfig,
      onSuccess = (_: Any) => inflightRequestsSemaphore.release(),
      onFailure = (ex) => {
        inflightRequestsSemaphore.release()
        LOGGER.error("Fail to write service metadata to cassandra after {} retry attempts", cfg.retryConfig.maxRetries, ex)
      })
  }


  private def shouldFlush(): Boolean = {
    cfg.flushIntervalInSec == 0 || Instant.now().minusSeconds(cfg.flushIntervalInSec).isAfter(lastWriteInstant)
  }

  /**
    * writes the span buffer to external store like cassandra, elastic, or kafka
    *
    * @param traceId          trace id
    * @param packedSpanBuffer compressed serialized bytes of the span buffer object
    * @param isLastSpanBuffer tells if this is the last record, so the writer can flush
    */
  override def writeAsync(traceId: String, packedSpanBuffer: PackedMessage[SpanBuffer], isLastSpanBuffer: Boolean): Unit = {
    try {
      val serviceMetadataStatements = statementBuilder.getAndUpdateServiceMetadata(
        packedSpanBuffer.protoObj.getChildSpansList.asScala,
        shouldFlush())

      /* write service metadata in cassandra */
      if(serviceMetadataStatements.nonEmpty) {
        serviceMetadataStatements.foreach(execute)
        lastWriteInstant = Instant.now()
      }
    } catch {
      case ex: Exception =>
        LOGGER.error("Fail to write the service metadata to cassandra with exception", ex)
        writeFailures.mark()
    }
  }

  override def close(): Unit = ()
}
