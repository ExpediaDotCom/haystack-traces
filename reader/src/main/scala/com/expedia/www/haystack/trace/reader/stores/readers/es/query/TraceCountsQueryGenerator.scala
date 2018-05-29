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
import io.searchbox.core.Count
import org.apache.lucene.search.join.ScoreMode
import org.elasticsearch.index.query.QueryBuilders.{nestedQuery, rangeQuery}
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.JavaConverters._

class TraceCountsQueryGenerator(indexNamePrefix: String,
                                indexType: String,
                                indexHourBucket: Int,
                                indexHourTtl: Int,
                                nestedDocName: String,
                                indexConfiguration: WhitelistIndexFieldConfiguration) extends QueryGenerator(nestedDocName, indexConfiguration) {
  def generate(request: TraceCountsRequest, startTime: Long): Count = {
    require(request.getStartTime > 0)
    require(request.getEndTime > 0)
    require(request.getInterval > 0)

    // base search query
    val query = createQuery(request.getFieldsList)

    // add filter for time bucket being searched
    // TODO move filter out of nested query
    query
      .filter(nestedQuery(nestedDocName, rangeQuery(withBaseDoc(START_TIME_KEY_NAME))
        .gte(startTime)
        .lte(startTime + request.getInterval), ScoreMode.None))

    // create count query string
    val countQueryString = new SearchSourceBuilder()
      .query(query)
      .toString

    // create ES count query
    new Count.Builder()
      .query(countQueryString)
      .addIndex(getESIndexes(request.getStartTime, request.getEndTime, indexNamePrefix, indexHourBucket, indexHourTtl).asJava)
      .addType(indexType)
      .build()
  }
}
