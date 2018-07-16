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

package com.expedia.www.haystack.trace.reader.stores.readers.es.query

import com.expedia.open.tracing.api.TraceCountsRequest
import com.expedia.www.haystack.trace.commons.clients.es.document.TraceIndexDoc
import com.expedia.www.haystack.trace.commons.config.entities.WhitelistIndexFieldConfiguration
import com.expedia.www.haystack.trace.reader.config.entities.ElasticSearchConfiguration
import io.searchbox.core.Search
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.JavaConverters._


object TraceCountsQueryGenerator {
  val COUNT_HISTOGRAM_NAME = "countagg"
}

class TraceCountsQueryGenerator(esConfig: ElasticSearchConfiguration,
                                nestedDocName: String,
                                indexConfiguration: WhitelistIndexFieldConfiguration)
  extends QueryGenerator(nestedDocName, indexConfiguration) {

  import TraceCountsQueryGenerator._

  def generate(request: TraceCountsRequest, useSpecificIndices: Boolean): Search = {
    require(request.getStartTime > 0)
    require(request.getEndTime > 0)
    require(request.getInterval > 0)

    if (useSpecificIndices) {
      generate(request)
    } else {
      new Search.Builder(buildQueryString(request))
        .addIndex(esConfig.indexNamePrefix)
        .addType(esConfig.indexType)
        .build()
    }
  }

  def generate(request: TraceCountsRequest): Search = {
    require(request.getStartTime > 0)
    require(request.getEndTime > 0)
    require(request.getInterval > 0)

    // create ES count query
    val targetIndicesToSearch = getESIndexes(
      request.getStartTime,
      request.getEndTime,
      esConfig.indexNamePrefix,
      esConfig.indexHourBucket,
      esConfig.indexHourTtl).asJava

    new Search.Builder(buildQueryString(request))
      .addIndex(targetIndicesToSearch)
      .addType(esConfig.indexType)
      .build()
  }

  private def buildQueryString(request: TraceCountsRequest): String = {
    val query =
      if(request.hasFilterExpression)
        createExpressionTreeBasedQuery(request.getFilterExpression)
      else
        createFilterFieldBasedQuery(request.getFieldsList)

    val aggregation = AggregationBuilders
      .histogram(COUNT_HISTOGRAM_NAME)
      .field(TraceIndexDoc.START_TIME_KEY_NAME)
      .interval(request.getInterval)
      .extendedBounds(request.getStartTime, request.getEndTime)

    new SearchSourceBuilder()
      .query(query)
      .aggregation(aggregation)
      .size(0)
      .toString
  }
}