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

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.Semaphore

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.indexer.config.entities.{ElasticSearchConfiguration, IndexConfiguration}
import com.expedia.www.haystack.trace.indexer.metrics.{AppMetricNames, MetricsSupport}
import com.expedia.www.haystack.trace.indexer.writers.es.index.document.IndexDocumentGenerator
import io.searchbox.action.Action
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._
import io.searchbox.indices.template.PutTemplate
import io.searchbox.params.Parameters
import org.slf4j.LoggerFactory

import scala.util.Try

class ElasticSearchWriter(esConfig: ElasticSearchConfiguration, indexConf: IndexConfiguration)
  extends MetricsSupport with AutoCloseable {

  private val LOGGER = LoggerFactory.getLogger(classOf[ElasticSearchWriter])

  // meter that measures the write failures
  private val esWriteFailureMeter = metricRegistry.meter(AppMetricNames.ES_WRITE_FAILURE)

  // a timer that measures the amount of time it takes to complete one bulk write
  private val esWriteTime = metricRegistry.timer(AppMetricNames.ES_WRITE_TIME)

  // converts a span into an indexable document
  private val documentGenerator = new IndexDocumentGenerator(indexConf)

  // this semaphore controls the parallel writes to cassandra
  private val inflightRequestsSemaphore = new Semaphore(esConfig.maxInFlightRequests, true)

  // initialize the elastic search client
  private val esClient: JestClient = {
    LOGGER.info("Initializing the http elastic search client with endpoint={}", esConfig.endpoint)
    val factory = new JestClientFactory()

    factory.setHttpClientConfig(
      new HttpClientConfig.Builder(esConfig.endpoint)
        .multiThreaded(true)
        .connTimeout(esConfig.connectionTimeoutMillis)
        .readTimeout(esConfig.readTimeoutMillis)
        .build())
    factory.getObject
  }

  if(esConfig.indexTemplateJson.isDefined) applyTemplate(esConfig.indexTemplateJson.get)

  override def close(): Unit = {
    LOGGER.info("Closing the elastic search client now.")
    Try(esClient.shutdownClient())
  }

  /**
    * converts the spans to an index document and writes to elastic search. Also if the parallel writes
    * exceed the max inflight requests, then we block and this puts backpressure on upstream
    * @param traceId trace id
    * @param spanBuffer list of spans belonging to this traceId
    * @return
    */
  def write(traceId: String, spanBuffer: SpanBuffer): Unit = {
    var isSemaphoreAcquired = false

    try {
      createIndexAction(traceId, spanBuffer, indexName()) match {
        case Some(indexRequest) =>
          inflightRequestsSemaphore.acquire()
          isSemaphoreAcquired = true

          // execute the request async
          esClient.executeAsync(indexRequest, new TraceIndexResultHandler(inflightRequestsSemaphore, esWriteTime.time()))
        case _ => // skip indexing this span buffer
      }
    } catch {
      case ex: Exception =>
        if(isSemaphoreAcquired) inflightRequestsSemaphore.release()
        esWriteFailureMeter.mark()
        LOGGER.error("Failed to write spans to elastic search with exception", ex)
    }
  }

  private def createIndexAction(traceId: String, spanBuffer: SpanBuffer, indexName: String): Option[Action[DocumentResult]] = {
    // add all the spans as one document
    val idxDocument = documentGenerator.createIndexDocument(traceId, spanBuffer)

    idxDocument match {
      case Some(doc) =>
        Some(new Index.Builder(doc.json)
          .id(doc.id)
          .index(indexName)
          .`type`(esConfig.indexType)
          .setParameter(Parameters.CONSISTENCY, esConfig.consistencyLevel)
          .setParameter(Parameters.OP_TYPE, "create")
          .build())
      case _ =>
        LOGGER.warn("Skipping the span buffer record for index operation!")
        None
    }
  }

  private def applyTemplate(templateJson: String) {
    val putTemplateRequest = new PutTemplate.Builder("haystack-template", templateJson).build()
    val result = esClient.execute(putTemplateRequest)
    if(!result.isSucceeded) {
      throw new RuntimeException(s"Fail to apply the following template to elastic search with reason=${result.getErrorMessage}")
    }
  }

  // creates an index name based on current date. following example illustrates the naming convention of
  // elastic search indices:
  // haystack-span-2017-08-30
  private def indexName(): String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    s"${esConfig.indexNamePrefix}-${formatter.format(new Date())}"
  }
}
