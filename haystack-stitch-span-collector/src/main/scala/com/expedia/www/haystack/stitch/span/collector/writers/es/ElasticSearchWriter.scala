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

import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.stitch.span.collector.config.entities.{ElasticSearchConfiguration, IndexConfiguration}
import com.expedia.www.haystack.stitch.span.collector.metrics.AppMetricNames
import com.expedia.www.haystack.stitch.span.collector.writers.es.index.document.IndexDocumentGenerator
import com.expedia.www.haystack.stitch.span.collector.writers.{StitchedSpanDataElement, StitchedSpanWriter}
import io.searchbox.action.BulkableAction
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._
import io.searchbox.params.Parameters
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}
import scala.util.Try

class ElasticSearchWriter(esConfig: ElasticSearchConfiguration, indexConf: IndexConfiguration) extends StitchedSpanWriter {
  private val LOGGER = LoggerFactory.getLogger(classOf[ElasticSearchWriter])

  private val esWriteDocsHistogram = metricRegistry.histogram(AppMetricNames.ES_WRITE_DOCS)
  private val esWriteFailureMeter = metricRegistry.meter(AppMetricNames.ES_WRITE_FAILURE)
  private val esWriteTime = metricRegistry.timer(AppMetricNames.ES_WRITE_TIME)

  private val spanIndexer = new IndexDocumentGenerator(indexConf)

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

  override def close(): Unit = {
    LOGGER.info("Closing the elastic search client now.")
    Try(esClient.shutdownClient())
  }

  override def write(elems: Seq[StitchedSpanDataElement]): Future[_] = {
    try {
      val request = buildIndexingRequest(elems)
      val promise = Promise[Boolean]()
      esClient.executeAsync(request, new SpanIndexResultHandler(promise, esWriteTime.time()))
      esWriteDocsHistogram.update(elems.size)
      promise.future
    } catch {
      case ex: Exception =>
        esWriteFailureMeter.mark()
        LOGGER.error("Failed to write stitched spans with exception", ex)
        Future.failed(ex)
    }
  }

  private def buildIndexingRequest(elems: Seq[StitchedSpanDataElement]): Bulk = {
    val bulkActions = new Bulk.Builder()

    // create the index name
    val indexName = createIndexName()

    for(el <- elems;
        action = createIndexAction(el.stitchedSpan, indexName); if action.isDefined) {
      bulkActions.addAction(action.get)
    }

    bulkActions.build()
  }

  private def createIndexAction(stitchedSpan: StitchedSpan, indexName: String): Option[BulkableAction[DocumentResult]] = {
    // add all the spans as one document
    spanIndexer.createIndexDocument(stitchedSpan) match {
      case Some(doc) =>
        Some(new Index.Builder(doc.stitchedSpanIndexJson)
          .id(doc.id)
          .index(indexName)
          .`type`(esConfig.indexType)
          .setParameter(Parameters.CONSISTENCY, esConfig.consistencyLevel)
          .setParameter(Parameters.OP_TYPE, "create")
          .build())
      case _ => None
    }
  }

  private def createIndexName(): String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    s"${esConfig.indexNamePrefix}-${formatter.format(new Date())}"
  }
}
