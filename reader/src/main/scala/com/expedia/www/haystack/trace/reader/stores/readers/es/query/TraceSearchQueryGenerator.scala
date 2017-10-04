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

import com.expedia.open.tracing.api.TracesSearchRequest
import io.searchbox.core.Search
import io.searchbox.strings.StringUtils
import org.apache.lucene.search.join.ScoreMode
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query._
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.JavaConversions._

class TraceSearchQueryGenerator(indexNamePrefix: String, indexType: String, nestedDocName: String) {
  private val START_TIME_FIELD = "startTime"

  def generate(request: TracesSearchRequest): Search = {
    require(request.getStartTime > 0)
    require(request.getEndTime > 0)
    require(request.getLimit > 0)

    new Search.Builder(buildQueryString(request))
      .addIndex(s"$indexNamePrefix")
      .addType(indexType)
      .build()
  }

  private def buildQueryString(request: TracesSearchRequest) = {
    new SearchSourceBuilder()
      .query(boolQuery.must(createNestedQuery(request)))
      .size(request.getLimit)
      .toString
  }

  private def createNestedQuery(request: TracesSearchRequest): NestedQueryBuilder = {
    val nestedBoolQuery: BoolQueryBuilder = boolQuery()

    // add all fields as term sub query
    val subQueries: Seq[QueryBuilder] =
      for (field <- request.getFieldsList;
           termQuery = buildTermQuery(field.getName, field.getValue); if termQuery.isDefined) yield termQuery.get
    subQueries.foreach(nestedBoolQuery.filter)

    // set time range window
    nestedBoolQuery
      .must(rangeQuery(withBaseDoc(START_TIME_FIELD))
        .gte(request.getStartTime)
        .lte(request.getEndTime))

    nestedQuery(nestedDocName, nestedBoolQuery, ScoreMode.Avg)
  }

  private def buildTermQuery(key: String, value: String): Option[TermQueryBuilder] = {
    if (StringUtils.isBlank(value)) {
      None
    }
    else {
      Some(termQuery(withBaseDoc(key), value))
    }
  }

  private def withBaseDoc(field: String) = s"$nestedDocName.$field"
}
