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
import com.expedia.www.haystack.trace.commons.clients.es.document.TraceIndexDoc.START_TIME_KEY_NAME
import com.expedia.www.haystack.trace.commons.config.entities.WhitelistIndexFieldConfiguration
import io.searchbox.core.Search
import org.elasticsearch.index.query.QueryBuilders.rangeQuery
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.JavaConverters._

class TraceCountsQueryGenerator(indexNamePrefix: String,
                                indexType: String,
                                nestedDocName: String,
                                indexConfiguration: WhitelistIndexFieldConfiguration) extends QueryGenerator(nestedDocName, indexConfiguration) {
  def generate(request: TraceCountsRequest): Search = {
    new Search.Builder(buildQueryString(request))
      .addIndex(s"__$indexNamePrefix*")
      .addType(indexType)
      .build()
  }

  private def buildQueryString(request: TraceCountsRequest): String = {
    new SearchSourceBuilder()
      .aggregation(createNestedAggregationQueryWithNestedSearch(request))
      .size(0)
      .toString
  }

  protected def createNestedAggregationQueryWithNestedSearch(request: TraceCountsRequest): AggregationBuilder = {
    // create search query
    val query = createBoolQuery(request.getFieldsList.asScala)

    // add range filter for time range
    query.filter(rangeQuery(withBaseDoc(START_TIME_KEY_NAME))
      .gte(request.getStartTime)
      .lte(request.getEndTime))

    // nested aggregation post search
    new NestedAggregationBuilder(nestedDocName, nestedDocName)
      .subAggregation(
        new FilterAggregationBuilder(s"$nestedDocName", query)
          .subAggregation(
            new HistogramAggregationBuilder("__count_per_interval").interval(request.getInterval).field(withBaseDoc("starttime"))
          )
      )
  }
}
