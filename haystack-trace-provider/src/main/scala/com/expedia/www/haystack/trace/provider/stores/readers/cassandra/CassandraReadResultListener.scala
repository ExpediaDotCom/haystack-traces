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

package com.expedia.www.haystack.trace.provider.stores.readers.cassandra

import com.codahale.metrics.{Meter, Timer}
import com.datastax.driver.core.{ResultSet, ResultSetFuture, Row}
import com.expedia.open.tracing.internal.Trace
import com.expedia.www.haystack.trace.provider.exceptions.TraceNotFoundException
import com.expedia.www.haystack.trace.provider.metrics.MetricsSupport
import com.expedia.www.haystack.trace.provider.stores.serde.SpanBufferDeserializer
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Promise
import scala.util.{Failure, Success, Try}

object CassandraReadResultListener extends MetricsSupport {
  protected val LOGGER: Logger = LoggerFactory.getLogger(classOf[CassandraReadResultListener])
  protected val readFailures: Meter = metricRegistry.meter("cassandra.read.failure")
  protected val readWarnings: Meter = metricRegistry.meter("cassandra.read.warnings")
}

class CassandraReadResultListener(asyncResult: ResultSetFuture,
                                  timer: Timer.Context,
                                  failure: Meter,
                                  promise: Promise[Trace]) extends Runnable {

  import CassandraReadResultListener._

  val deserializer = new SpanBufferDeserializer

  override def run(): Unit = {
    timer.close()

    Try(asyncResult.get)
      .flatMap(tryGetTraceRow)
      .flatMap(tryDeserialize)
    match {
      case Success(trace) =>
        promise.success(trace)
      case Failure(ex) =>
        LOGGER.error("Failed in reading the record from cassandra", ex)
        readFailures.mark()
        promise.failure(ex)
    }
  }

  private def tryGetTraceRow(resultSet: ResultSet): Try[Row] = {
    val row: Row = resultSet.one()
    row match {
      case null => Failure(new TraceNotFoundException)
      case row => Success(row)
    }
  }

  private def tryDeserialize(row: Row): Try[Trace] = {
    val rawSpans = row.getBytes(Schema.STITCHED_SPANS_COLUMNE_NAME).array()
    deserializer.deserialize(rawSpans) match {
      case Success(spans) =>
        Success(Trace.newBuilder()
          .setTraceId(spans.getTraceId)
          .addAllChildSpans(spans.getChildSpansList)
          .build())
      case Failure(ex) =>
        Failure(ex)
    }
  }
}
