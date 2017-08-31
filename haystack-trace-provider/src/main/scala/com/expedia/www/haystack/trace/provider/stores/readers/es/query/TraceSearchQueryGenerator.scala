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

package com.expedia.www.haystack.trace.provider.stores.readers.es.query

import com.expedia.open.tracing.internal.TracesSearchRequest
import io.searchbox.core.Search
import io.searchbox.strings.StringUtils
import org.apache.lucene.search.join.ScoreMode
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, QueryBuilder, RangeQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.JavaConversions._

class TraceSearchQueryGenerator(indexNamePrefix: String, indexType: String) {
  private val NESTED_DOC_NAME = "spans"

  def generate(request: TracesSearchRequest): Search = {
    new Search.Builder(buildQueryString(request))
      .addIndex(s"$indexNamePrefix*") // TODO add specific indexes based on given time window
      .addType(indexType)
      .build()
  }

  // TODO further improve and optimize query
  private def buildQueryString(request: TracesSearchRequest) = {
    val subQueries: List[QueryBuilder] = List(
      buildMatchQuery("service", request.getServiceName),
      buildMatchQuery("operation", request.getServiceName),
      buildRangeQuery("duration", request.getMinDuration, request.getMaxDuration),
      buildRangeQuery("startTime", request.getStartTime, request.getEndTime),
      buildTagMatchQuery(request)
    ).flatten

    val nestedMatchQuery: BoolQueryBuilder = subQueries
      .foldLeft(boolQuery())((boolQuery, q) => boolQuery.must(q))

    new SearchSourceBuilder()
      .query(boolQuery.must(nestedQuery(NESTED_DOC_NAME, nestedMatchQuery, ScoreMode.Avg)))
      .sort(withBaseDoc("startTime"))  // TODO default to a random sorting algo instead
      .size(request.getLimit)
      .toString
  }

  private def buildTagMatchQuery(request: TracesSearchRequest) = {
    request.getFieldsList
      .foldLeft(boolQuery())((query, field) => query.must(matchQuery(field.getName, field.getVStr)))
  }

  private def buildMatchQuery(key: String, value: String): Option[MatchQueryBuilder] = {
    if (StringUtils.isBlank(value)) None
    else Some(matchQuery(withBaseDoc(key), value))
  }

  private def buildRangeQuery(key: String, min: Long, max: Long): Option[RangeQueryBuilder] = {
    if (max > 0) Some(rangeQuery(withBaseDoc(key)).from(min).to(max))
    else None
  }

  private def withBaseDoc(field: String) = {
    s"$NESTED_DOC_NAME.$field"
  }
}
