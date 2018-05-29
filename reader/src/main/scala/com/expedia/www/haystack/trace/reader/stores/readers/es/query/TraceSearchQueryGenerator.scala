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
import com.expedia.www.haystack.trace.commons.clients.es.document.TraceIndexDoc._
import com.expedia.www.haystack.trace.commons.config.entities.WhitelistIndexFieldConfiguration
import io.searchbox.core.Search
import org.apache.lucene.search.join.ScoreMode
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.{FieldSortBuilder, SortOrder}

import scala.collection.JavaConverters._

class TraceSearchQueryGenerator(indexNamePrefix: String,
                                indexType: String,
                                indexHourBucket: Int,
                                indexHourTtl: Int,
                                nestedDocName: String,
                                indexConfiguration: WhitelistIndexFieldConfiguration)
  extends QueryGenerator(nestedDocName, indexConfiguration) {

  def generate(request: TracesSearchRequest): Search = {
    require(request.getStartTime > 0)
    require(request.getEndTime > 0)
    require(request.getLimit > 0)

    new Search.Builder(buildQueryString(request))
      .addIndex(getESIndexes(request.getStartTime, request.getEndTime, indexNamePrefix, indexHourBucket, indexHourTtl).asJava)
      .addType(indexType)
      .build()
  }

  private def buildQueryString(request: TracesSearchRequest): String = {
    val query = createQuery(request.getFieldsList)
    // set time range window
    query
      .filter(nestedQuery(nestedDocName, rangeQuery(withBaseDoc(START_TIME_KEY_NAME))
        .gte(request.getStartTime)
        .lte(request.getEndTime), ScoreMode.None))

    new SearchSourceBuilder()
      .query(query)
      .sort(new FieldSortBuilder(withBaseDoc(START_TIME_KEY_NAME)).order(SortOrder.DESC).setNestedPath(nestedDocName))
      .size(request.getLimit)
      .toString
  }
}
