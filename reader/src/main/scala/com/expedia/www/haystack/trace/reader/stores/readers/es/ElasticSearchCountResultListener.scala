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

package com.expedia.www.haystack.trace.reader.stores.readers.es

import com.codahale.metrics.{Meter, Timer}
import com.expedia.www.haystack.trace.reader.exceptions.ElasticSearchClientError
import com.expedia.www.haystack.trace.reader.stores.readers.es.ElasticSearchCountResultListener._
import com.google.gson.Gson
import io.searchbox.client.JestResultHandler
import io.searchbox.core.{Count, CountResult}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Promise

object ElasticSearchCountResultListener {
  protected val LOGGER: Logger = LoggerFactory.getLogger(classOf[ElasticSearchCountResultListener])

  protected def is2xx(code: Int): Boolean = (code / 100) == 2
}

class ElasticSearchCountResultListener(request: Count,
                                       promise: Promise[CountResult],
                                       timer: Timer.Context,
                                       failure: Meter) extends JestResultHandler[CountResult] {

  override def completed(result: CountResult): Unit = {
    timer.close()

    if (!is2xx(result.getResponseCode)) {
      val ex = ElasticSearchClientError(result.getResponseCode, result.getJsonString)
      LOGGER.error(s"Failed in reading from elasticsearch for request='${request.getData(new Gson())}'", ex)
      failure.mark()
      promise.failure(ex)
    } else {
      promise.success(result)
    }
  }

  override def failed(ex: Exception): Unit = {
    LOGGER.error(s"Failed in reading from elasticsearch for request=${request.getData(new Gson())}", ex)
    failure.mark()
    timer.close()
    promise.failure(ex)
  }
}
