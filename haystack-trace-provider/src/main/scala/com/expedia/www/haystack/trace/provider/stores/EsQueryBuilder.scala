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

package com.expedia.www.haystack.trace.provider.stores

import com.expedia.open.tracing.internal.TracesSearchRequest
import io.searchbox.core.Search
import org.apache.lucene.search.join.ScoreMode
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.JavaConversions._

trait EsQueryBuilder {
  private val NESTED_DOC_NAME = "spans"
  private val SORT_BY = withBaseDoc("startTime") // TODO default to a random sorting algo instead

  private def withBaseDoc(field: String) = {
    s"$NESTED_DOC_NAME.$field"
  }

  private def buildTagMatchQuery(request: TracesSearchRequest) = {
    request.getFieldsList
      .foldLeft(boolQuery())((query, field) => query.must(matchQuery(field.getName, field.getVStr)))
  }

  // TODO further improve/optimize query
  private def buildQueryString(request: TracesSearchRequest) = {
    val queryBuilder: BoolQueryBuilder = boolQuery
      .must(
        nestedQuery(NESTED_DOC_NAME,
          buildTagMatchQuery(request)
            .must(matchQuery(withBaseDoc("service"), request.getServiceName))
            .must(matchQuery(withBaseDoc("operation"), request.getOperationName))
            .must(rangeQuery(withBaseDoc("duration")).from(request.getMinDuration).to(request.getMaxDuration))
            .must(rangeQuery(withBaseDoc("startTime")).from(request.getStartTime).to(request.getEndTime)),
          ScoreMode.Avg))

    new SearchSourceBuilder()
      .query(queryBuilder)
      .sort(SORT_BY)
      .size(request.getLimit)
      .fetchSource("_id", null) // only fetch document ids from es, dont need any other
      .toString
  }

  def buildSelectQuery(request: TracesSearchRequest, indexNamePrefix: String, indexType: String): Search = {
    new Search.Builder(buildQueryString(request))
      .addIndex(s"$indexNamePrefix*") // TODO add specific indexes based on given time window
      .addType(indexType)
      .build()
  }
}
