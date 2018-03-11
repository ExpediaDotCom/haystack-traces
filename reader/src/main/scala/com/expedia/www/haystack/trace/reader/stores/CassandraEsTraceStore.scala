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
import com.expedia.www.haystack.trace.commons.config.entities.{CassandraConfiguration, WhitelistIndexFieldConfiguration}
import com.expedia.www.haystack.trace.reader.config.entities.{ElasticSearchConfiguration, FieldValuesCacheConfiguration}
import com.expedia.www.haystack.trace.reader.metrics.{AppMetricNames, MetricsSupport}
import com.expedia.www.haystack.trace.reader.stores.readers.cassandra.CassandraReader
import com.expedia.www.haystack.trace.reader.stores.readers.es.ElasticSearchReader
import com.expedia.www.haystack.trace.reader.stores.readers.es.query.{FieldValuesQueryGenerator, TraceSearchQueryGenerator}
import io.searchbox.core.SearchResult
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

class CassandraEsTraceStore(cassandraConfiguration: CassandraConfiguration,
                            esConfiguration: ElasticSearchConfiguration,
                            indexConfiguration: WhitelistIndexFieldConfiguration,
                            cacheConfiguration: FieldValuesCacheConfiguration)(implicit val executor: ExecutionContextExecutor)
  extends TraceStore with MetricsSupport {
  implicit val formats = DefaultFormats

  private val ES_FIELD_AGGREGATIONS = "aggregations"
  private val ES_FIELD_BUCKETS = "buckets"
  private val ES_TRACE_ID_KEY = "traceid"
  private val ES_FIELD_KEY = "key"
  private val ES_NESTED_DOC_NAME = "spans"

  private val LOGGER = LoggerFactory.getLogger(classOf[ElasticSearchReader])
  private val traceRejected = metricRegistry.meter(AppMetricNames.SEARCH_TRACE_REJECTED)

  private val cassandraReader: CassandraReader = new CassandraReader(cassandraConfiguration)
  private val esReader: ElasticSearchReader = new ElasticSearchReader(esConfiguration)

  private val traceSearchQueryGenerator = new TraceSearchQueryGenerator(esConfiguration.indexNamePrefix, esConfiguration.indexType, ES_NESTED_DOC_NAME)
  private val fieldValuesQueryGenerator = new FieldValuesQueryGenerator(esConfiguration.indexNamePrefix, esConfiguration.indexType, ES_NESTED_DOC_NAME)

  private val cache = new FieldValuesCache(cacheConfiguration, fieldValuesFromDatabase)(executor)

  override def searchTraces(request: TracesSearchRequest): Future[Seq[Trace]] = {
    esReader
      .search(traceSearchQueryGenerator.generate(request))
      .flatMap(extractTraces)
  }

  private def extractTraces(result: SearchResult): Future[Seq[Trace]] = {

    // go through each hit and fetch trace for parsed traceId
    val sourceList = result.getSourceAsStringList
    if(sourceList != null && sourceList.size() > 0) {
      val traceFutures = sourceList
        .map(source => extractTraceIdFromSource(source))
        .filter(!_.isEmpty)
        .toSet[String]
        .toSeq
        .map(id => getTrace(id))

      // wait for all Futures to complete and then map them to Traces
      Future
        .sequence(liftToTry(traceFutures))
        .map(_.flatMap(retrieveTriedTrace))
    } else {
      Future.successful(Nil)
    }
  }

  private def extractTraceIdFromSource(source: String): String = {
    (parse(source) \ ES_TRACE_ID_KEY).extract[String]
  }

  override def getTrace(traceId: String): Future[Trace] = cassandraReader.readTrace(traceId)

  private def retrieveTriedTrace(mayBeTrace: Try[Trace]): Option[Trace] = {
    mayBeTrace match {
      case Success(trace) =>
        Some(trace)
      case Failure(ex) =>
        LOGGER.warn("traceId not found in cassandra, rejected searched trace", ex)
        traceRejected.mark()
        None
    }
  }

  // convert all Futures to Try to make sure they all complete
  private def liftToTry(traceFutures: Seq[Future[Trace]]): Seq[Future[Try[Trace]]] = traceFutures.map { f =>
    f.map(Try(_)).recover { case t: Throwable => Failure(t) }
  }

  override def getFieldNames(): Future[Seq[String]] = {
    Future.successful(indexConfiguration.whitelistIndexFields.map(_.name))
  }

  override def getFieldValues(request: FieldValuesRequest): Future[Seq[String]] = {
    cache.get(request)
  }

  private def fieldValuesFromDatabase(request: FieldValuesRequest): Future[Seq[String]] = {
    esReader
      .search(fieldValuesQueryGenerator.generate(request))
      .map(extractFieldValues(_, request.getFieldName.toLowerCase))
  }

  private def extractFieldValues(result: SearchResult, fieldName: String): List[String] =
    result
      .getJsonObject
      .getAsJsonObject(ES_FIELD_AGGREGATIONS)
      .getAsJsonObject(ES_NESTED_DOC_NAME)
      .getAsJsonObject(fieldName)
      .getAsJsonArray(ES_FIELD_BUCKETS)
      .map(element => element.getAsJsonObject.get(ES_FIELD_KEY).getAsString)
      .toList

  override def close(): Unit = {
    cassandraReader.close()
    esReader.close()
  }
}
