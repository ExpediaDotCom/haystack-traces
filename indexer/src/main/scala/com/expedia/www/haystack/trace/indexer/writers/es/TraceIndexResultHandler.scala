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
import com.expedia.www.haystack.trace.indexer.metrics.{AppMetricNames, MetricsSupport}
import io.searchbox.client.JestResultHandler
import io.searchbox.core.DocumentResult
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Promise

object TraceIndexResultHandler extends MetricsSupport {
  // elastic search reports with conflict response code if a document exists with same index, type and id
  // @see See [[https://www.elastic.co/guide/en/elasticsearch/guide/current/create-doc.html]] for more details
  protected val ERROR_CODE_DUPLICATE_RECORD = 409

  protected val LOGGER: Logger = LoggerFactory.getLogger(TraceIndexResultHandler.getClass)
  protected val esDuplicateDocumentMeter: Meter = metricRegistry.meter(AppMetricNames.ES_WRITE_DUPLICATES)
  protected val esWriteFailureMeter: Meter = metricRegistry.meter(AppMetricNames.ES_WRITE_FAILURE)
}

class TraceIndexResultHandler(promise: Promise[Boolean],
                              timer: Timer.Context) extends JestResultHandler[DocumentResult] {

  import TraceIndexResultHandler._

  /**
    * this callback is invoked when the elastic search writes is completed with success or warnings
    * @param result bulk result
    */
  def completed(result: DocumentResult): Unit = {
    timer.stop()
    updatePromiseWithResult(result)
  }

  /**
    * this callback is invoked when the writes to elastic search fail completely
    * @param ex the exception contains the reason of failure
    */
  def failed(ex: Exception): Unit = {
    LOGGER.error("Fail to write all the documents in elastic search with reason:", ex)
    timer.stop()
    esWriteFailureMeter.mark()
    promise.failure(ex)
  }

  // we gather the duplicate/conflict records as a metric and a log statement.
  // this may happen only if we rewrite the batch of records due to any failure, since we rely on kafka's at least
  // once semantics, hence there may arise cases where we may end up retry writing the records that were earlier written
  // successfully.
  private def updatePromiseWithResult(result: DocumentResult): Unit = {
    if (ERROR_CODE_DUPLICATE_RECORD == result.getResponseCode) {
      esDuplicateDocumentMeter.mark()
      promise.success(true)
    } else if (result.getErrorMessage != null) {
      esWriteFailureMeter.mark()
      val errorMessage = s"Index operation has failed with id=${result.getId} status=${result.getResponseCode} and error=${result.getErrorMessage}"
      LOGGER.error(errorMessage)
      promise.failure(new RuntimeException(errorMessage))
    } else {
      promise.success(true)
    }
  }
}
