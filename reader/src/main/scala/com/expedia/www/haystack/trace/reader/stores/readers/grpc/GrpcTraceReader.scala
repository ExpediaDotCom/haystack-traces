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

package com.expedia.www.haystack.trace.reader.stores.readers.grpc

import com.expedia.open.tracing.api.Trace
import com.expedia.open.tracing.backend.{ReadSpansRequest, StorageBackendGrpc}
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trace.commons.config.entities.TraceBackendClientConfiguration
import com.expedia.www.haystack.trace.reader.metrics.AppMetricNames
import io.grpc.ManagedChannelBuilder
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}

class GrpcTraceReader(config: TraceBackendClientConfiguration)
                     (implicit val dispatcher: ExecutionContextExecutor) extends MetricsSupport with AutoCloseable {
  private val LOGGER = LoggerFactory.getLogger(classOf[GrpcTraceReader])

  private val readTimer = metricRegistry.timer(AppMetricNames.BACKEND_READ_TIME)
  private val readFailures = metricRegistry.meter(AppMetricNames.BACKEND_READ_FAILURES)
  private val tracesFailures = metricRegistry.meter(AppMetricNames.BACKEND_TRACES_FAILURE)
  private val channel = ManagedChannelBuilder.forAddress(config.host, config.port)
    .usePlaintext(true)
    .build()
  val client: StorageBackendGrpc.StorageBackendFutureStub = StorageBackendGrpc.newFutureStub(channel)


  def readTraces(traceIds: List[String]): Future[Seq[Trace]] = {
    val timer = readTimer.time()
    val promise = Promise[Seq[Trace]]

    try {
      val readSpansRequest = ReadSpansRequest.newBuilder().addAllTraceIds(traceIds.asJava).build()
      val futureResponse = client.readSpans(readSpansRequest)
      futureResponse.addListener(new ReadSpansResponseListener(futureResponse, promise, timer, readFailures, tracesFailures, traceIds.size), dispatcher)
      promise.future
    } catch {
      case ex: Exception =>
        readFailures.mark()
        timer.stop()
        LOGGER.error("Failed to read raw traces with exception", ex)
        Future.failed(ex)
    }
  }

  override def close(): Unit = {
    channel.shutdown()
  }
}
