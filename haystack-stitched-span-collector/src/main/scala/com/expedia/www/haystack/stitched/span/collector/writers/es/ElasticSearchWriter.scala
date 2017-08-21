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

package com.expedia.www.haystack.stitched.span.collector.writers.es

import java.text.SimpleDateFormat
import java.util.Date

import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.stitched.span.collector.config.entities.{ElasticSearchConfiguration, IndexConfiguration}
import com.expedia.www.haystack.stitched.span.collector.metrics.AppMetricNames
import com.expedia.www.haystack.stitched.span.collector.writers.es.index.document.IndexDocumentGenerator
import com.expedia.www.haystack.stitched.span.collector.writers.{StitchedSpanDataRecord, StitchedSpanWriter}
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

  // histogram that measures the number of documents written to elastic search
  private val esWriteDocsHistogram = metricRegistry.histogram(AppMetricNames.ES_WRITE_DOCS)

  // meter that measures the write failures
  private val esWriteFailureMeter = metricRegistry.meter(AppMetricNames.ES_WRITE_FAILURE)

  // a timer that measures the amount of time it takes to complete one bulk write
  private val esWriteTime = metricRegistry.timer(AppMetricNames.ES_WRITE_TIME)

  // converts a span into an indexable document
  private val stitchedSpanIndexGenerator = new IndexDocumentGenerator(indexConf)

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

  override def close(): Unit = {
    LOGGER.info("Closing the elastic search client now.")
    Try(esClient.shutdownClient())
  }

  /**
    * writes the stitched span records to elastic search asynchronously
    * @param elems list of stitched span records
    * @return Future that contains the success or failure of the write operation
    */
  override def write(elems: Seq[StitchedSpanDataRecord]): Future[_] = {
    try {
      val request = buildBulkIndexingRequest(elems)
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

  /**
    * Builds the bulk index request for the list of stitched spans.
    * Each stitched span record is converted to an index document for elastic search write
    * @param records a set of stitched span records
    * @return
    */
  private def buildBulkIndexingRequest(records: Seq[StitchedSpanDataRecord]): Bulk = {
    val bulkActions = new Bulk.Builder()

    for(rec <- records;
        action = createIndexAction(rec.stitchedSpan, indexName()); if action.isDefined) {
      bulkActions.addAction(action.get)
    }

    bulkActions.build()
  }

  private def createIndexAction(stitchedSpan: StitchedSpan, indexName: String): Option[BulkableAction[DocumentResult]] = {
    // add all the spans as one document
    val idxDocument = stitchedSpanIndexGenerator.createIndexDocument(stitchedSpan)

    idxDocument match {
      case Some(doc) =>
        Some(new Index.Builder(doc.stitchedSpanIndexJson)
          .id(doc.id)
          .index(indexName)
          .`type`(esConfig.indexType)
          .setParameter(Parameters.CONSISTENCY, esConfig.consistencyLevel)
          .setParameter(Parameters.OP_TYPE, "create")
          .build())
      case _ =>
        LOGGER.info("Fail to convert the stitched span record to an index document!")
        None
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
