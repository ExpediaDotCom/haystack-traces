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

package com.expedia.www.haystack.stitch.span.collector.writers.es

import java.text.SimpleDateFormat
import java.util.Date

import com.codahale.metrics.Timer
import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.stitch.span.collector.config.entities.{ElasticSearchConfiguration, IndexConfiguration}
import com.expedia.www.haystack.stitch.span.collector.metrics.AppMetricNames
import com.expedia.www.haystack.stitch.span.collector.writers.StitchedSpanWriter
import io.searchbox.action.BulkableAction
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory, JestResultHandler}
import io.searchbox.core.{Bulk, BulkResult, DocumentResult, Index}
import io.searchbox.params.Parameters
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.{Future, Promise}

class ElasticSearchWriter(config: ElasticSearchConfiguration) extends StitchedSpanWriter {
  private val LOGGER = LoggerFactory.getLogger(classOf[ElasticSearchWriter])

  private val esWriteDocsHistogram = metricRegistry.histogram(AppMetricNames.ES_WRITE_DOCS)
  private val esWriteFailureMeter = metricRegistry.meter(AppMetricNames.ES_WRITE_FAILURE)
  private val esDuplicateDocumentMeter = metricRegistry.meter(AppMetricNames.ES_WRITE_DUPLICATES)
  private val esWriteTime = metricRegistry.timer(AppMetricNames.ES_WRITE_TIME)
  private val ERROR_CODE_DUPLICATE_RECORD = 409

  private val spanIndexer = new SpanIndexer(IndexConfiguration(Set()))

  private val esClient: JestClient = {
    val host = esHost()
    LOGGER.info("Initializing the http elastic search client with host={}", host)
    val factory = new JestClientFactory()

    factory.setHttpClientConfig(
      new HttpClientConfig.Builder(host)
        .multiThreaded(true)
        .connTimeout(config.connectionTimeoutMillis)
        .readTimeout(config.readTimeoutMillis)
        .build())
    factory.getObject
  }

  private def esHost(): String = {
    val configHostName = config.port match {
      case Some(port) => config.host + ":" + port
      case _ => config.host
    }
    if (configHostName.startsWith("http://") || configHostName.startsWith("https://")) configHostName else "http://" + configHostName
  }

  override def close(): Unit = {
    LOGGER.info("Closing the elastic search client now.")
    esClient.shutdownClient()
  }

  override def write(stitchedSpans: Seq[StitchedSpan]): Future[_] = {
    try {
      val (docs, request) = buildIndexingRequest(stitchedSpans)
      val promise = Promise[Boolean]()
      esClient.executeAsync(request, new EsResponseCallback(promise, esWriteTime.time()))
      esWriteDocsHistogram.update(docs)
      promise.future
    } catch {
      case ex: Exception =>
        esWriteFailureMeter.mark()
        LOGGER.error("Failed to write stitched spans with exception", ex)
        Future.failed(ex)
    }
  }

  private def buildIndexingRequest(stitchedSpans: Seq[StitchedSpan]): (Long, Bulk) = {
    var docsCount = 0L
    val bulkActions = new Bulk.Builder()

    // create the index name
    val indexName = createIndexName()

    for(sp <- stitchedSpans;
        parent = createParentIndexOp(sp, indexName);
        child = createChildIndexOp(sp, indexName)) {
      bulkActions.addAction(parent).addAction(child)
      docsCount = docsCount + 2
    }

    (docsCount, bulkActions.build())
  }

  private def createParentIndexOp(stitchedSpan: StitchedSpan, indexName: String): BulkableAction[DocumentResult] = {
    new Index.Builder("{}")
      .id(stitchedSpan.getTraceId)
      .index(indexName)
      .`type`(config.parentType)
      .setParameter(Parameters.CONSISTENCY, "one")
      .setParameter(Parameters.OP_TYPE, "create")
      .build()
  }

  private def createChildIndexOp(stitchedSpan: StitchedSpan, indexName: String): BulkableAction[DocumentResult] = {
    // add all the spans as one document
    val indexDocument = spanIndexer.createIndexDocument(stitchedSpan.getChildSpansList)

    new Index.Builder(indexDocument.doc)
      .id(indexDocument.id)
      .index(indexName)
      .`type`(config.childType)
      .setParameter(Parameters.CONSISTENCY, config.consistencyLevel)
      .setParameter(Parameters.PARENT, stitchedSpan.getTraceId)
      .setParameter(Parameters.OP_TYPE, "create")
      .build()
  }

  private def createIndexName(): String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    s"${config.indexNamePrefix}-${formatter.format(new Date())}"
  }

  private class EsResponseCallback(promise: Promise[Boolean],
                                   timer: Timer.Context) extends JestResultHandler[BulkResult] {

    private def readBulkResult(result: BulkResult): Unit = {
      result.getFailedItems.foreach(item => {
        if (ERROR_CODE_DUPLICATE_RECORD == item.status) {
          esDuplicateDocumentMeter.mark()
        }
        else {
          esWriteFailureMeter.mark()
          LOGGER.error(s"Bulk operation has failed for id=${item.id}, status=${item.status} and error=${item.error}")
        }
      })
    }

    def completed(result: BulkResult): Unit = {
      timer.stop()
      readBulkResult(result)
      // mark the promise as success even for partial failures.
      // partial failures may happen for duplicate records and is not a fatal error
      promise.success(true)
    }

    def failed(ex: Exception): Unit = {
      LOGGER.error("Fail to write all the documents in elastic search with reason:", ex)
      timer.stop()
      esWriteFailureMeter.mark()
      promise.failure(ex)
    }
  }
}
