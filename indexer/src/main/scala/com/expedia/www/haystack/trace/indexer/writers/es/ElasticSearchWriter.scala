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
import java.util.{Calendar, Locale, TimeZone}

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.commons.retries.RetryOperation._
import com.expedia.www.haystack.trace.commons.config.entities.WhitelistIndexFieldConfiguration
import com.expedia.www.haystack.trace.commons.packer.PackedMessage
import com.expedia.www.haystack.trace.indexer.config.entities.ElasticSearchConfiguration
import com.expedia.www.haystack.trace.indexer.metrics.AppMetricNames
import com.expedia.www.haystack.trace.indexer.writers.TraceWriter
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._
import io.searchbox.params.Parameters
import org.apache.commons.lang3.time.FastDateFormat
import org.slf4j.LoggerFactory

import scala.util.Try

object ElasticSearchWriterUtils {

  private val timezone = TimeZone.getTimeZone("UTC")
  private val format = FastDateFormat.getInstance("yyyy-MM-dd", timezone, Locale.US)

  // creates an index name based on current date. following example illustrates the naming convention of
  // elastic search indices:
  // haystack-span-2017-08-30-1
  def indexName(prefix: String, indexHourBucket: Int): String = {
    val currentTime = Calendar.getInstance(timezone)
    val bucket: Int = currentTime.get(Calendar.HOUR_OF_DAY) / indexHourBucket
    s"$prefix-${format.format(currentTime.getTime)}-$bucket"
  }
}
class ElasticSearchWriter(esConfig: ElasticSearchConfiguration, whitelistFieldConfig: WhitelistIndexFieldConfiguration)
  extends TraceWriter with MetricsSupport {

  implicit val formats = org.json4s.DefaultFormats

  private val LOGGER = LoggerFactory.getLogger(classOf[ElasticSearchWriter])

  // meter that measures the write failures
  private val esWriteFailureMeter = metricRegistry.meter(AppMetricNames.ES_WRITE_FAILURE)

  // a timer that measures the amount of time it takes to complete one bulk write
  private val esWriteTime = metricRegistry.timer(AppMetricNames.ES_WRITE_TIME)

  // converts a span into an indexable document
  private val documentGenerator = new IndexDocumentGenerator(whitelistFieldConfig)

  // this semaphore controls the parallel writes to index store
  private val inflightRequestsSemaphore = new Semaphore(esConfig.maxInFlightBulkRequests, true)

  // initialize the elastic search client
  private lazy val esClient: JestClient = {
    LOGGER.info("Initializing the http elastic search client with endpoint={}", esConfig.endpoint)

    val factory = new JestClientFactory()
    val builder = new HttpClientConfig.Builder(esConfig.endpoint)
      .multiThreaded(true)
      .connTimeout(esConfig.connectionTimeoutMillis)
      .readTimeout(esConfig.readTimeoutMillis)
      .defaultMaxTotalConnectionPerRoute(esConfig.maxConnectionsPerRoute)

    if (esConfig.username.isDefined && esConfig.password.isDefined){
      builder.defaultCredentials(esConfig.username.get, esConfig.password.get)
    }

    factory.setHttpClientConfig(builder.build())
    val client = factory.getObject
    new IndexTemplateHandler(esClient, esConfig.indexTemplateJson, esConfig.indexType, whitelistFieldConfig).run()
    client
  }

  private val bulkBuilder = new ThreadSafeBulkBuilder(esConfig.maxDocsInBulk, esConfig.maxBulkDocSizeInBytes)

  override def close(): Unit = {
    LOGGER.info("Closing the elastic search client now.")
    Try(esClient.shutdownClient())
  }

  /**
    * converts the spans to an index document and writes to elastic search. Also if the parallel writes
    * exceed the max inflight requests, then we block and this puts backpressure on upstream
    *
    * @param traceId          trace id
    * @param packedSpanBuffer  list of spans belonging to this traceId - packed bytes of span buffer
    * @param isLastSpanBuffer tells if this is the last record, so the writer can flush
    * @return
    */
  override def writeAsync(traceId: String, packedSpanBuffer: PackedMessage[SpanBuffer], isLastSpanBuffer: Boolean): Unit = {
    var isSemaphoreAcquired = false

    try {
      addIndexOperation(traceId, packedSpanBuffer.protoObj, ElasticSearchWriterUtils.indexName(esConfig.indexNamePrefix, esConfig.indexHourBucket), isLastSpanBuffer) match {
        case Some(bulkToDispatch) =>
          inflightRequestsSemaphore.acquire()
          isSemaphoreAcquired = true

          // execute the request async with retry
          withRetryBackoff((retryCallback) => {
            esClient.executeAsync(bulkToDispatch, new ElasticSearchResultHandler(esWriteTime.time(), esWriteFailureMeter, retryCallback))
          },
            esConfig.retryConfig,
            onSuccess = (_: Any) => inflightRequestsSemaphore.release(),
            onFailure = (ex) => {
              inflightRequestsSemaphore.release()
              LOGGER.error("Fail to write to ES after {} retry attempts", esConfig.retryConfig.maxRetries, ex)
            })
        case _ =>
      }
    } catch {
      case ex: Exception =>
        if (isSemaphoreAcquired) inflightRequestsSemaphore.release()
        esWriteFailureMeter.mark()
        LOGGER.error("Failed to write spans to elastic search with exception", ex)
    }
  }

  private def addIndexOperation(traceId: String, spanBuffer: SpanBuffer, indexName: String, forceBulkCreate: Boolean): Option[Bulk] = {
    // add all the spans as one document
    val idxDocument = documentGenerator.createIndexDocument(traceId, spanBuffer)

    idxDocument match {
      case Some(doc) =>
        val action: Index = new Index.Builder(doc.json)
          .index(indexName)
          .`type`(esConfig.indexType)
          .setParameter(Parameters.CONSISTENCY, esConfig.consistencyLevel)
          .build()
        bulkBuilder.addAction(action, doc.json.getBytes("utf-8").length, forceBulkCreate)
      case _ =>
        LOGGER.warn("Skipping the span buffer record for index operation for traceId={}!", traceId)
        None
    }
  }
}
