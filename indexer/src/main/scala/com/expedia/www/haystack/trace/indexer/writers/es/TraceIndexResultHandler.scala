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

package com.expedia.www.haystack.trace.indexer.writers.es

import java.util.concurrent.Semaphore

import com.codahale.metrics.{Meter, Timer}
import com.expedia.www.haystack.trace.indexer.metrics.{AppMetricNames, MetricsSupport}
import io.searchbox.client.JestResultHandler
import io.searchbox.core.DocumentResult
import org.slf4j.{Logger, LoggerFactory}

object TraceIndexResultHandler extends MetricsSupport {
  protected val LOGGER: Logger = LoggerFactory.getLogger(TraceIndexResultHandler.getClass)
  protected val esWriteFailureMeter: Meter = metricRegistry.meter(AppMetricNames.ES_WRITE_FAILURE)
}

class TraceIndexResultHandler(inflightRequestsSemaphore: Semaphore, timer: Timer.Context)
  extends JestResultHandler[DocumentResult] {

  import TraceIndexResultHandler._

  /**
    * this callback is invoked when the elastic search writes is completed with success or warnings
    *
    * @param result bulk result
    */
  def completed(result: DocumentResult): Unit = {
    inflightRequestsSemaphore.release()
    timer.stop()

    if (result.getErrorMessage != null) {
      esWriteFailureMeter.mark()
      LOGGER.error(s"Index operation has failed with id=${result.getId} status=${result.getResponseCode} " +
        s"and error=${result.getErrorMessage}")
    }
  }

  /**
    * this callback is invoked when the writes to elastic search fail completely
    *
    * @param ex the exception contains the reason of failure
    */
  def failed(ex: Exception): Unit = {
    inflightRequestsSemaphore.release()
    timer.stop()

    LOGGER.error("Fail to write all the documents in elastic search with reason:", ex)
    esWriteFailureMeter.mark()
  }
}
