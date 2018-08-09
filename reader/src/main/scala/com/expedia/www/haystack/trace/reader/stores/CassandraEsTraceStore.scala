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

package com.expedia.www.haystack.trace.reader.stores

import com.expedia.open.tracing.api._
import com.expedia.www.haystack.trace.commons.clients.cassandra.{CassandraClusterFactory, CassandraSession}
import com.expedia.www.haystack.trace.commons.clients.es.document.TraceIndexDoc
import com.expedia.www.haystack.trace.commons.config.entities.{CassandraConfiguration, WhitelistIndexFieldConfiguration}
import com.expedia.www.haystack.trace.reader.config.entities.{ElasticSearchConfiguration, ServiceMetadataReadConfiguration}
import com.expedia.www.haystack.trace.reader.metrics.MetricsSupport
import com.expedia.www.haystack.trace.reader.stores.readers.ServiceMetadataReader
import com.expedia.www.haystack.trace.reader.stores.readers.cassandra.CassandraTraceReader
import com.expedia.www.haystack.trace.reader.stores.readers.es.ElasticSearchReader
import com.expedia.www.haystack.trace.reader.stores.readers.es.query.{FieldValuesQueryGenerator, TraceCountsQueryGenerator, TraceSearchQueryGenerator}
import io.searchbox.core.SearchResult
import org.elasticsearch.index.IndexNotFoundException
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContextExecutor, Future}

class CassandraEsTraceStore(cassandraConfig: CassandraConfiguration,
                            serviceMetadataConfig: ServiceMetadataReadConfiguration,
                            esConfig: ElasticSearchConfiguration,
                            indexConfig: WhitelistIndexFieldConfiguration)(implicit val executor: ExecutionContextExecutor)
  extends TraceStore with MetricsSupport with ResponseParser {

  private val LOGGER = LoggerFactory.getLogger(classOf[ElasticSearchReader])

  private val cassandraSession = new CassandraSession(cassandraConfig, new CassandraClusterFactory)
  private val cassandraReader: CassandraTraceReader = new CassandraTraceReader(cassandraSession, cassandraConfig)
  private val esReader: ElasticSearchReader = new ElasticSearchReader(esConfig)
  private val serviceMetadataReader: ServiceMetadataReader = new ServiceMetadataReader(cassandraSession, serviceMetadataConfig)

  private val traceSearchQueryGenerator = new TraceSearchQueryGenerator(esConfig, ES_NESTED_DOC_NAME, indexConfig)
  private val traceCountsQueryGenerator = new TraceCountsQueryGenerator(esConfig, ES_NESTED_DOC_NAME, indexConfig)
  private val fieldValuesQueryGenerator = new FieldValuesQueryGenerator(esConfig, ES_NESTED_DOC_NAME, indexConfig)

  private val esCountTraces = (request: TraceCountsRequest, useSpecificIndices: Boolean) => {
    esReader.count(traceCountsQueryGenerator.generate(request, useSpecificIndices))
  }

  private val esSearchTraces = (request: TracesSearchRequest, useSpecificIndices: Boolean) => {
    esReader.search(traceSearchQueryGenerator.generate(request, useSpecificIndices))
  }

  private def handleResult(result: Future[SearchResult],
                           retryFunc: () => Future[SearchResult]): Future[SearchResult] = {
    result.recoverWith {
      case _: IndexNotFoundException => retryFunc()
    }
  }

  override def searchTraces(request: TracesSearchRequest): Future[Seq[Trace]] = {
    // search ES with specific indices
    val esResult = esSearchTraces(request, true)
    // handle the response and retry in case of IndexNotFoundException
    handleResult(esResult, () => esSearchTraces(request, false)).flatMap(result => extractTraces(result))
  }

  private def extractTraces(result: SearchResult): Future[Seq[Trace]] = {

    // go through each hit and fetch trace for parsed traceId
    val sourceList = result.getSourceAsStringList
    if (sourceList != null && sourceList.size() > 0) {
      val traceIds = sourceList
        .asScala
        .map(source => extractTraceIdFromSource(source))
        .filter(!_.isEmpty)
        .toSet[String] // de-dup traceIds
        .toList

      cassandraReader.readRawTraces(traceIds)
    } else {
      Future.successful(Nil)
    }
  }

  override def getTrace(traceId: String): Future[Trace] = cassandraReader.readTrace(traceId)

  override def getFieldNames(): Future[Seq[String]] = {
    Future.successful(indexConfig.whitelistIndexFields.map(_.name).distinct.sorted)
  }

  private def readFromServiceMetadata(request: FieldValuesRequest): Option[Future[Seq[String]]] = {
    if (!serviceMetadataConfig.enabled) return None

    if (request.getFieldName.toLowerCase == TraceIndexDoc.SERVICE_KEY_NAME && request.getFiltersCount == 0) {
      Some(serviceMetadataReader.fetchAllServiceNames())
    } else if (request.getFieldName.toLowerCase == TraceIndexDoc.OPERATION_KEY_NAME
      && (request.getFiltersCount == 1)
      && request.getFiltersList.get(0).getName.toLowerCase == TraceIndexDoc.SERVICE_KEY_NAME) {
      Some(serviceMetadataReader.fetchServiceOperations(request.getFilters(0).getValue))
    } else {
      LOGGER.info("read from service metadata request isn't served by cassandra")
      None
    }
  }

  override def getFieldValues(request: FieldValuesRequest): Future[Seq[String]] = {
    readFromServiceMetadata(request).getOrElse(
      esReader
        .search(fieldValuesQueryGenerator.generate(request))
        .map(extractFieldValues(_, request.getFieldName.toLowerCase)))
  }

  override def getTraceCounts(request: TraceCountsRequest): Future[TraceCounts] = {
    // search ES with specific indices
    val esResponse = esCountTraces(request, true)

    // handle the response and retry in case of IndexNotFoundException
    handleResult(esResponse, () => esCountTraces(request, false))
      .map(result => mapSearchResultToTraceCount(request.getStartTime, request.getEndTime, result))
  }

  override def getRawTraces(request: RawTracesRequest): Future[Seq[Trace]] = {
    cassandraReader.readRawTraces(request.getTraceIdList.asScala.toList)
  }

  override def close(): Unit = {
    cassandraReader.close()
    esReader.close()
    cassandraSession.close()
  }
}
