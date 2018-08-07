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

import com.codahale.metrics.{Meter, Timer}
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.{ResultSet, ResultSetFuture, Row}
import com.expedia.open.tracing.api.Trace
import com.expedia.www.haystack.commons.health.HealthController
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraTableSchema
import com.expedia.www.haystack.trace.reader.exceptions.TraceNotFoundException
import com.expedia.www.haystack.trace.reader.stores.readers.cassandra.CassandraReadRawTracesResultListener._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Promise
import scala.util.{Failure, Success, Try}

object CassandraReadRawTracesResultListener {
  protected val LOGGER: Logger = LoggerFactory.getLogger(classOf[CassandraReadRawTracesResultListener])
}

class CassandraReadRawTracesResultListener(asyncResult: ResultSetFuture,
                                           timer: Timer.Context,
                                           failure: Meter,
                                           promise: Promise[Seq[Trace]]) extends Runnable {
  override def run(): Unit = {
    timer.close()

    Try(asyncResult.get)
      .flatMap(tryGetTraceRows)
      .flatMap(tryDeserialize)
    match {
      case Success(traces) =>
        promise.success(traces)
      case Failure(ex) =>
        if (fatalError(ex)) {
          LOGGER.error("Fatal error in reading from cassandra, tearing down the app", ex)
          HealthController.setUnhealthy()
        } else {
          LOGGER.error("Failed in reading the record from cassandra", ex)
        }
        failure.mark()
        promise.failure(ex)
    }
  }

  private def fatalError(ex: Throwable): Boolean = {
    if (ex.isInstanceOf[NoHostAvailableException]) true else ex.getCause != null && fatalError(ex.getCause)
  }

  private def tryGetTraceRows(resultSet: ResultSet): Try[Seq[Row]] = {
    val rows = resultSet.all().asScala
    if (rows.isEmpty) Failure(new TraceNotFoundException) else Success(rows)
  }

  private def tryDeserialize(rows: Seq[Row]): Try[Seq[Trace]] = {
    val traceBuilderMap = new mutable.HashMap[String, Trace.Builder]()
    var deserFailed: Failure[Seq[Trace]] = null

    rows.foreach(row => {
      CassandraTableSchema.extractSpanBufferFromRow(row) match {
        case Success(sBuffer) =>
          traceBuilderMap.getOrElseUpdate(sBuffer.getTraceId, Trace.newBuilder().setTraceId(sBuffer.getTraceId)).addAllChildSpans(sBuffer.getChildSpansList)
        case Failure(cause) => deserFailed = Failure(cause)
      }
    })
    if (deserFailed == null) Success(traceBuilderMap.values.map(_.build).toSeq) else deserFailed
  }
}
