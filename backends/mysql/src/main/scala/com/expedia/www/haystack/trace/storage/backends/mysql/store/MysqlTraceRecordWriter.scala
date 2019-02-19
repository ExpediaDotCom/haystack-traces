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

package com.expedia.www.haystack.trace.storage.backends.mysql.store

import java.io.ByteArrayInputStream
import java.sql.{Connection, Timestamp}
import java.util.concurrent.atomic.AtomicInteger

import com.expedia.open.tracing.backend.TraceRecord
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.commons.retries.RetryOperation._
import com.expedia.www.haystack.trace.storage.backends.mysql.client.SqlConnectionManager
import com.expedia.www.haystack.trace.storage.backends.mysql.config.entities.MysqlConfiguration
import com.expedia.www.haystack.trace.storage.backends.mysql.metrics.AppMetricNames
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success}

class MysqlTraceRecordWriter(config: MysqlConfiguration, sqlConnectionManager: SqlConnectionManager)(implicit val dispatcher: ExecutionContextExecutor)
  extends MetricsSupport {

  private val LOGGER = LoggerFactory.getLogger(classOf[MysqlTraceRecordWriter])
  private lazy val writeTimer = metricRegistry.timer(AppMetricNames.MYSQL_WRITE_TIME)
  private lazy val writeFailures = metricRegistry.meter(AppMetricNames.MYSQL_WRITE_FAILURE)

  val writeRecordSql = "insert into spans values(?,?,?)"

  private def execute(record: TraceRecord): Future[Unit] = {

    val promise = Promise[Unit]

    // execute the request async with retry
    withRetryBackoff(retryCallback => {
      var connection:Connection = null
      try {
        val timer = writeTimer.time()
        connection = sqlConnectionManager.getConnection
        val statement = connection.prepareStatement(writeRecordSql)
        statement.setString(1, record.getTraceId)
        statement.setBlob(2, new ByteArrayInputStream(record.getSpans.toByteArray))
        statement.setTimestamp(3, new Timestamp(record.getTimestamp))
        statement.execute()
        retryCallback.onResult(statement.getResultSet)
      } catch {
        case ex: Exception => retryCallback.onError(ex, retry = true)
      }
      finally {
        if (connection != null) connection.close()
      }
    },
      config.retryConfig,
      onSuccess = (_: Any) => promise.success(),
      onFailure = ex => {
        writeFailures.mark()
        LOGGER.error(s"Fail to write to mysql after ${config.retryConfig.maxRetries} retry attempts for ${record.getTraceId}", ex)
        promise.failure(ex)
      })
    promise.future
  }

  /**
    * writes the traceId and its spans to mysql. Use the current timestamp as the sort key for the writes to same
    * TraceId. Also if the parallel writes exceed the max inflight requests, then we block and this puts backpressure on
    * upstream
    *
    * @param traceRecords : trace records which need to be written
    * @return
    */
  def writeTraceRecords(traceRecords: List[TraceRecord]): Future[Unit] = {
    val promise = Promise[Unit]
    val writableRecordsLatch = new AtomicInteger(traceRecords.size)
    traceRecords.foreach(record => {
      /* write spanBuffer for a given traceId */
      execute(record).onComplete {
        case Success(_) => if (writableRecordsLatch.decrementAndGet() == 0) {
          promise.success()
        }
        case Failure(ex) =>
          //TODO: We fail the response only if the last mysql write fails, ideally we should be failing if any of the mysql writes fail
          if (writableRecordsLatch.decrementAndGet() == 0) {
            promise.failure(ex)
          }
      }
    })
    promise.future

  }
}
