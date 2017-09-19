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
import com.expedia.www.haystack.trace.indexer.writers.TraceWriter
import io.searchbox.action.BulkableAction
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._
import io.searchbox.indices.template.PutTemplate
import io.searchbox.params.Parameters
import org.slf4j.LoggerFactory

import scala.util.Try

class ElasticSearchWriter(esConfig: ElasticSearchConfiguration, indexConf: IndexConfiguration)
  extends TraceWriter with MetricsSupport {

  private val LOGGER = LoggerFactory.getLogger(classOf[ElasticSearchWriter])

  // meter that measures the write failures
  private val esWriteFailureMeter = metricRegistry.meter(AppMetricNames.ES_WRITE_FAILURE)

  // a timer that measures the amount of time it takes to complete one bulk write
  private val esWriteTime = metricRegistry.timer(AppMetricNames.ES_WRITE_TIME)

  // converts a span into an indexable document
  private val documentGenerator = new IndexDocumentGenerator(indexConf)

  // this semaphore controls the parallel writes to cassandra
  private val inflightRequestsSemaphore = new Semaphore(esConfig.maxInFlightBulkRequests, true)

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

  private val bulkActions = new BulkActionBuilder()

  if(esConfig.indexTemplateJson.isDefined) applyTemplate(esConfig.indexTemplateJson.get)

  override def close(): Unit = {
    LOGGER.info("Closing the elastic search client now.")
    Try(esClient.shutdownClient())
  }

  /**
    * converts the spans to an index document and writes to elastic search. Also if the parallel writes
    * exceed the max inflight requests, then we block and this puts backpressure on upstream
    * @param traceId trace id
    * @param spanBuffer list of spans belonging to this traceId - span buffer
    * @param spanBufferBytes list of spans belonging to this traceId - serialized bytes of span buffer
    * @param isLastSpanBuffer tells if this is the last record, so the writer can flush
    * @return
    */
  override def writeAsync(traceId: String, spanBuffer: SpanBuffer, spanBufferBytes: Array[Byte], isLastSpanBuffer: Boolean): Unit = {
    var isSemaphoreAcquired = false

    try {
      addIndexOperation(traceId, spanBuffer, indexName())

      if(isLastSpanBuffer || bulkActions.isReadyForDispatch) {
        inflightRequestsSemaphore.acquire()
        isSemaphoreAcquired = true

        // execute the request async
        esClient.executeAsync(bulkActions.buildAndReset(), new TraceIndexResultHandler(inflightRequestsSemaphore, esWriteTime.time()))
      }
    } catch {
      case ex: Exception =>
        if(isSemaphoreAcquired) inflightRequestsSemaphore.release()
        esWriteFailureMeter.mark()
        LOGGER.error("Failed to write spans to elastic search with exception", ex)
    }
  }

  private def addIndexOperation(traceId: String, spanBuffer: SpanBuffer, indexName: String): Unit = {
    // add all the spans as one document
    val idxDocument = documentGenerator.createIndexDocument(traceId, spanBuffer)

    idxDocument match {
      case Some(doc) =>
        val action: Index = new Index.Builder(doc.json)
          .id(doc.id)
          .index(indexName)
          .`type`(esConfig.indexType)
          .setParameter(Parameters.CONSISTENCY, esConfig.consistencyLevel)
          .setParameter(Parameters.OP_TYPE, "create")
          .build()
        bulkActions.addAction(action, doc.json.getBytes("utf-8").length)
      case _ =>
        LOGGER.warn("Skipping the span buffer record for index operation!")
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

  private sealed class BulkActionBuilder {
    private var bulk = new Bulk.Builder()
    private var docsCount = 0
    private var docsSizeInBytes = 0

    def buildAndReset(): Bulk = {
      val result = bulk.build()
      bulk = new Bulk.Builder
      docsCount = 0
      docsSizeInBytes = 0
      result
    }

    def addAction(action: BulkableAction[DocumentResult], sizeInBytes: Int): Unit = {
      bulk.addAction(action)
      this.docsSizeInBytes += sizeInBytes
      this.docsCount += 1
    }

    def isReadyForDispatch: Boolean = {
      docsCount >= esConfig.maxDocsInBulk || docsSizeInBytes >= esConfig.maxBulkDocSizeInKb * 1000
    }
  }
}
