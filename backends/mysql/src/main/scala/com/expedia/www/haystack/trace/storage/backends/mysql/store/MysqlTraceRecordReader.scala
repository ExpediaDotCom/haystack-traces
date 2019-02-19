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

package com.expedia.www.haystack.trace.storage.backends.mysql.store

import java.sql.Connection

import com.expedia.open.tracing.backend.TraceRecord
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trace.storage.backends.mysql.client.MysqlTableSchema._
import com.expedia.www.haystack.trace.storage.backends.mysql.client.SqlConnectionManager
import com.expedia.www.haystack.trace.storage.backends.mysql.config.entities.ClientConfiguration
import com.expedia.www.haystack.trace.storage.backends.mysql.metrics.AppMetricNames
import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}

class MysqlTraceRecordReader(config: ClientConfiguration, sqlConnectionManager: SqlConnectionManager)
                            (implicit val dispatcher: ExecutionContextExecutor) extends MetricsSupport {
  private val LOGGER = LoggerFactory.getLogger(classOf[MysqlTraceRecordReader])
  val readRecordSql = "SELECT * FROM spans WHERE id = ?"
  private lazy val readTimer = metricRegistry.timer(AppMetricNames.MYSQL_READ_TIME)
  private lazy val readFailures = metricRegistry.meter(AppMetricNames.MYSQL_READ_FAILURES)

  def readTraceRecords(traceIds: List[String]): Future[Seq[TraceRecord]] = {
    val timer = readTimer.time()
    val promise = Promise[Seq[TraceRecord]]
    var connection: Connection = null
    try {
      connection = sqlConnectionManager.getConnection
      val statement = connection.prepareStatement(readRecordSql)
      statement.setString(1, traceIds.head)
      statement.execute()
      val results = statement.getResultSet
      var records: List[TraceRecord] = List()

      while (results.next()) {
        val spans = results.getBlob(SPANS_COLUMN_NAME)
        val record = TraceRecord.newBuilder()
          .setTraceId(results.getString(ID_COLUMN_NAME))
          .setSpans(ByteString.copyFrom(spans.getBytes(1, spans.length().toInt)))
          .setTimestamp(results.getTimestamp(TIMESTAMP_COLUMN_NAME).getTime)
          .build()
        records = record :: records

      }
      promise.success(records)
      promise.future
    }
    catch {
      case ex: Exception =>
        readFailures.mark()
        timer.stop()
        LOGGER.error("Failed to read raw traces with exception", ex)
        Future.failed(ex)
    }
    finally {
      if (connection != null) connection.close()
    }
  }

}