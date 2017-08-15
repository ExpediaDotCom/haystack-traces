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
import com.expedia.www.haystack.stitch.span.collector.writers.StitchedSpanWriter
import io.searchbox.action.BulkableAction
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._
import io.searchbox.params.Parameters
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}

class ElasticSearchWriter(config: ElasticSearchConfiguration) extends StitchedSpanWriter {
  private val LOGGER = LoggerFactory.getLogger(classOf[ElasticSearchWriter])

  private val esWriteDocsHistogram = metricRegistry.histogram(AppMetricNames.ES_WRITE_DOCS)
  private val esWriteFailureMeter = metricRegistry.meter(AppMetricNames.ES_WRITE_FAILURE)
  private val esWriteTime = metricRegistry.timer(AppMetricNames.ES_WRITE_TIME)

  private val spanIndexer = new SpanIndexDocumentGenerator(IndexConfiguration(Set()))

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
    val configHostName = config.host + ":" + config.port
    if (configHostName.startsWith("http://") || configHostName.startsWith("https://")) configHostName else "http://" + configHostName
  }

  override def close(): Unit = {
    LOGGER.info("Closing the elastic search client now.")
    esClient.shutdownClient()
  }

  override def write(stitchedSpans: Seq[StitchedSpan]): Future[_] = {
    try {
      val request = buildIndexingRequest(stitchedSpans)
      val promise = Promise[Boolean]()
      esClient.executeAsync(request, new SpanIndexResultHandler(promise, esWriteTime.time()))
      esWriteDocsHistogram.update(stitchedSpans.size)
      promise.future
    } catch {
      case ex: Exception =>
        esWriteFailureMeter.mark()
        LOGGER.error("Failed to write stitched spans with exception", ex)
        Future.failed(ex)
    }
  }

  private def buildIndexingRequest(stitchedSpans: Seq[StitchedSpan]): Bulk = {
    val bulkActions = new Bulk.Builder()

    // create the index name
    val indexName = createIndexName()

    for(sp <- stitchedSpans;
        op = createUpdateIndexOp(sp, indexName)) {
      bulkActions.addAction(op)
    }

    bulkActions.build()
  }

  private def createUpdateIndexOp(stitchedSpan: StitchedSpan, indexName: String): BulkableAction[DocumentResult] = {
    // add all the spans as one document
    val updateDocument = spanIndexer.create(stitchedSpan.getChildSpansList)

    new Update.Builder(updateDocument)
      .id(stitchedSpan.getTraceId)
      .index(indexName)
      .`type`(config.indexType)
      .setParameter(Parameters.CONSISTENCY, config.consistencyLevel)
      .setParameter(Parameters.PARENT, stitchedSpan.getTraceId)
      .setParameter(Parameters.OP_TYPE, "create")
      .build()
  }

  private def createIndexName(): String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    s"${config.indexNamePrefix}-${formatter.format(new Date())}"
  }
}
