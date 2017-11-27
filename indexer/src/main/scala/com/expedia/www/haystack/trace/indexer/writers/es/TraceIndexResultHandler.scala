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

import com.codahale.metrics.{Meter, Timer}
import com.expedia.www.haystack.trace.commons.retries.RetryOperation
import com.expedia.www.haystack.trace.indexer.metrics.{AppMetricNames, MetricsSupport}
import io.searchbox.client.JestResultHandler
import io.searchbox.core.BulkResult
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.rest.RestStatus
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

object TraceIndexResultHandler extends MetricsSupport {
  protected val LOGGER: Logger = LoggerFactory.getLogger(TraceIndexResultHandler.getClass)
  val esWriteFailureMeter: Meter = metricRegistry.meter(AppMetricNames.ES_WRITE_FAILURE)
}

class TraceIndexResultHandler(timer: Timer.Context, asyncRetryResult: RetryOperation.Callback)

  extends JestResultHandler[BulkResult] {

  import TraceIndexResultHandler._


  /**
    * this callback is invoked when the elastic search writes is completed with success or warnings
    *
    * @param result bulk result
    */
  def completed(result: BulkResult): Unit = {
    timer.close()

    // group the failed items as per status and log once for such a failed item
    if(result.getFailedItems != null) {
      result.getFailedItems.groupBy(_.status) foreach {
        case (statusCode, failedItems) =>
          esWriteFailureMeter.mark(failedItems.size)
          LOGGER.error(s"Index operation has failed with status=$statusCode, totalFailedItems=${failedItems.size}, " +
            s"errorReason=${failedItems.head.errorReason}, errorType=${failedItems.head.errorType}")
      }
    }
    asyncRetryResult.onResult(result)
  }

  /**
    * this callback is invoked when the writes to elastic search fail completely
    *
    * @param ex the exception contains the reason of failure
    */
  def failed(ex: Exception): Unit = {
    timer.close()
    esWriteFailureMeter.mark()
    LOGGER.error("Fail to write the documents in elastic search with reason:", ex)
    asyncRetryResult.onError(ex, shouldRetry(ex))
  }

  private def shouldRetry(ex: Exception): Boolean = {
    ex match {
      case e: ElasticsearchException => e.status() == RestStatus.TOO_MANY_REQUESTS
      case _ => false
    }
  }
}
