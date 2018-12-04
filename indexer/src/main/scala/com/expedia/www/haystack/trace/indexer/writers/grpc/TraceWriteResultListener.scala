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

package com.expedia.www.haystack.trace.indexer.writers.grpc

import java.util.concurrent.Future

import com.codahale.metrics.{Meter, Timer}
import com.expedia.open.tracing.backend.WriteSpansResponse
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.commons.retries.RetryOperation
import com.expedia.www.haystack.trace.indexer.metrics.AppMetricNames
import org.slf4j.{Logger, LoggerFactory}

object TraceWriteResultListener extends MetricsSupport {
  protected val LOGGER: Logger = LoggerFactory.getLogger(TraceWriteResultListener.getClass)
  protected val writeFailures: Meter = metricRegistry.meter(AppMetricNames.GRPC_WRITE_FAILURE)
}

class TraceWriteResultListener(result: Future[WriteSpansResponse],
                               timer: Timer.Context,
                               retryOp: RetryOperation.Callback) extends Runnable {

  /**
    * this is invoked when the grpc aysnc write completes.
    * We measure the time write operation takes and records any warnings or errors
    */

  import com.expedia.www.haystack.trace.indexer.writers.grpc.TraceWriteResultListener._

  override def run(): Unit = {
    try {
      timer.close()

      val writeSpanResponse = result.get()
      if (writeSpanResponse != null &&
        writeSpanResponse.getErrorMessage != null &&
        !writeSpanResponse.getErrorMessage.isEmpty) {
        LOGGER.error(s"Fail to write the record to trace backend with error {}", writeSpanResponse.getErrorMessage)
        writeFailures.mark()
      }
      if (retryOp != null) retryOp.onResult(writeSpanResponse)
    } catch {
      case ex: Exception =>
        LOGGER.error("Fail to write the record to trace backend with exception", ex)
        writeFailures.mark()
        if (retryOp != null) retryOp.onError(ex, retry = true)
    }
  }
}