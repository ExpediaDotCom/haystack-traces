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
import com.datastax.driver.core.ResultSetFuture
import com.expedia.open.tracing.internal.Trace
import com.expedia.www.haystack.trace.provider.exceptions.TraceNotFoundException
import com.expedia.www.haystack.trace.provider.metrics.MetricsSupport
import com.expedia.www.haystack.trace.provider.stores.serde.SpanBufferDeserializer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.concurrent.Promise

object CassandraReadResultListener extends MetricsSupport {
  protected val LOGGER: Logger = LoggerFactory.getLogger(classOf[CassandraReadResultListener])
  protected val writeFailures: Meter = metricRegistry.meter("cassandra.read.failure")
  protected val readWarnings: Meter = metricRegistry.meter("cassandra.read.warnings")
}

class CassandraReadResultListener(asyncResult: ResultSetFuture,
                                  timer: Timer.Context,
                                  failure: Meter,
                                  promise: Promise[Trace]) extends Runnable {
  import CassandraReadResultListener._

  val deserializer = new SpanBufferDeserializer

  override def run(): Unit = {
    try {
      timer.close()

      if (asyncResult.get != null &&
        asyncResult.get.getExecutionInfo != null &&
        asyncResult.get.getExecutionInfo.getWarnings != null &&
        asyncResult.get.getExecutionInfo.getWarnings.nonEmpty) {
        LOGGER.warn(s"Warning received in cassandra read {}", asyncResult.get.getExecutionInfo.getWarnings.toList.mkString(","))
      }

      val row = asyncResult.get().one()
      if(row == null) {
        throw new TraceNotFoundException
      }

      val trace = extractTrace(row.getBytes(Schema.STITCHED_SPANS_COLUMNE_NAME).array())
      promise.success(trace)
    } catch {
      case ex: Exception =>
        LOGGER.error("Fail to read the record from cassandra with exception", ex)
        writeFailures.mark()
        promise.failure(ex)
    }
  }

  private def extractTrace(rawSpans: Array[Byte]): Trace = {
    val spans = deserializer.deserialize(rawSpans)
    Trace.newBuilder()
      .setTraceId(spans.getTraceId)
      .addAllChildSpans(spans.getChildSpansList)
      .build()
  }
}
