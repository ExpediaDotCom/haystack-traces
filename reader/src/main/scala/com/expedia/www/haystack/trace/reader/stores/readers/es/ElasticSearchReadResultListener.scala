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
import com.expedia.www.haystack.trace.reader.stores.readers.es.ESUtils._
import com.expedia.www.haystack.trace.reader.stores.readers.es.ElasticSearchReadResultListener._
import io.searchbox.client.JestResultHandler
import io.searchbox.core.{Search, SearchResult}
import org.elasticsearch.index.IndexNotFoundException
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Promise

object ElasticSearchReadResultListener {
  protected val LOGGER: Logger = LoggerFactory.getLogger(classOf[ElasticSearchReadResultListener])
  val INDEX_NOT_FOUND_EXCEPTION = "index_not_found_exception"
  protected def is2xx(code: Int): Boolean = (code / 100) == 2
}

class ElasticSearchReadResultListener(request: Search,
                                      promise: Promise[ElasticSearchResult],
                                      timer: Timer.Context,
                                      failure: Meter) extends JestResultHandler[SearchResult] {

  override def completed(result: SearchResult): Unit = {
    timer.close()

    if (!is2xx(result.getResponseCode)) {
      if (result.getJsonString.toLowerCase.contains(INDEX_NOT_FOUND_EXCEPTION)) {
        val indexNotFoundEx = new IndexNotFoundException("Index not found exception, should retry", ElasticSearchClientError(result.getResponseCode, result.getJsonString))
        promise.success(FailedEsResult(indexNotFoundEx))
      } else {
        val ex = ElasticSearchClientError(result.getResponseCode, result.getJsonString)
        LOGGER.error(s"Failed in reading from elasticsearch for request='${request.toJson}'", ex)
        failure.mark()
        promise.failure(ex)
      }
    } else {
      promise.success(SuccessEsResult(result))
    }
  }

  override def failed(ex: Exception): Unit = {
    LOGGER.error(s"Failed in reading from elasticsearch for request=${request.toJson}", ex)
    failure.mark()
    timer.close()
    promise.failure(ex)
  }
}
