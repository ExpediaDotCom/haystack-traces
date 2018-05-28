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
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.QueryBuilders.rangeQuery
import org.elasticsearch.search.builder.SearchSourceBuilder

class TraceCountsQueryGenerator(indexNamePrefix: String,
                                indexType: String,
                                nestedDocName: String,
                                indexConfiguration: WhitelistIndexFieldConfiguration) extends QueryGenerator(nestedDocName, indexConfiguration) {
  def generate(request: TraceCountsRequest): Seq[Count] = {
    require(request.getStartTime > 0)
    require(request.getEndTime > 0)
    require(request.getInterval > 0)

    // base search query being used in all buckets
    val query = createQuery(request.getFieldsList)

    // loop through all the time buckets and create an ES Count query for all of them
    for (startTime <- roundTimestampToMinute(request.getStartTime) to roundTimestampToMinute(request.getEndTime) by request.getInterval)
      yield buildCountQuery(query, startTime, startTime + request.getInterval)
  }

  private def buildCountQuery(query: BoolQueryBuilder, startTime: Long, endTime: Long) = {
    // add filter for time bucket being searched
    query
      .filter(rangeQuery(withBaseDoc(START_TIME_KEY_NAME))
        .gte(startTime)
        .lte(endTime))

    // create count query string
    val countQueryString = new SearchSourceBuilder()
      .query(query)
      .toString

    // create ES count query
    new Count.Builder()
      .query(countQueryString)
      .addIndex(s"$indexNamePrefix")
      .addType(indexType)
      .build()
  }

  private def roundTimestampToMinute(timestampInMicro: Long): Long = {
    val fullMinutes = timestampInMicro / 60000000
    fullMinutes * 60000000
  }
}
