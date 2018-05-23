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

import com.expedia.open.tracing.api.{TraceCount, TraceCounts}
import io.searchbox.core.SearchResult
import org.json4s.jackson.JsonMethods.parse

import scala.collection.JavaConverters._
import scala.concurrent.Future
import org.json4s.DefaultFormats

trait ResponseParser {
  implicit val formats = DefaultFormats

  private val ES_FIELD_AGGREGATIONS = "aggregations"
  private val ES_FIELD_BUCKETS = "buckets"
  private val ES_FIELD_KEY = "key"
  private val ES_COUNT_PER_INTERVAL = "__count_per_interval"
  private val ES_AGG_DOC_COUNT = "doc_count"
  protected val ES_NESTED_DOC_NAME = "spans"
  private val ES_TRACE_ID_KEY = "traceid"

  protected def mapSearchResultToTraceCounts(result: SearchResult): Future[TraceCounts] = {
    val aggregation = result.getJsonObject
      .getAsJsonObject(ES_FIELD_AGGREGATIONS)
      .getAsJsonObject(ES_NESTED_DOC_NAME)
      .getAsJsonObject(ES_NESTED_DOC_NAME)
      .getAsJsonObject(ES_COUNT_PER_INTERVAL)

    val traceCounts = aggregation
      .getAsJsonArray(ES_FIELD_BUCKETS).asScala.map(
      element => TraceCount.newBuilder()
        .setTimestamp(element.getAsJsonObject.get(ES_FIELD_KEY).getAsLong)
        .setCount(element.getAsJsonObject.get(ES_AGG_DOC_COUNT).getAsLong)
        .build()
    ).asJava

    Future.successful(TraceCounts.newBuilder().addAllTraceCount(traceCounts).build())
  }

  protected def extractFieldValues(result: SearchResult, fieldName: String): List[String] = {
    val aggregations =
      result
        .getJsonObject
        .getAsJsonObject(ES_FIELD_AGGREGATIONS)
        .getAsJsonObject(ES_NESTED_DOC_NAME)
        .getAsJsonObject(fieldName)

    if (aggregations.has(ES_FIELD_BUCKETS))
      aggregations
        .getAsJsonArray(ES_FIELD_BUCKETS)
        .asScala
        .map(element => element.getAsJsonObject.get(ES_FIELD_KEY).getAsString)
        .toList
    else
      aggregations
        .getAsJsonObject(fieldName)
        .getAsJsonArray(ES_FIELD_BUCKETS)
        .asScala
        .map(element => element.getAsJsonObject.get(ES_FIELD_KEY).getAsString)
        .toList
  }

  protected def extractTraceIdFromSource(source: String): String = {
    (parse(source) \ ES_TRACE_ID_KEY).extract[String]
  }
}
