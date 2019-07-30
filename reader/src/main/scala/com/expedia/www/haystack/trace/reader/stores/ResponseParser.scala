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
import com.expedia.www.haystack.trace.commons.config.entities.IndexFieldType
import com.expedia.www.haystack.trace.reader.stores.readers.es.query.TraceCountsQueryGenerator
import io.searchbox.core.SearchResult
import io.searchbox.core.search.aggregation.HistogramAggregation
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, Formats}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Try

trait ResponseParser {
  protected implicit val formats: Formats = DefaultFormats + new EnumNameSerializer(IndexFieldType)

  private val ES_FIELD_AGGREGATIONS = "aggregations"
  private val ES_FIELD_BUCKETS = "buckets"
  private val ES_FIELD_KEY = "key"
  private val ES_COUNT_PER_INTERVAL = "__count_per_interval"
  private val ES_AGG_DOC_COUNT = "doc_count"
  protected val ES_NESTED_DOC_NAME = "spans"

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

  protected def mapSearchResultToTraceCount(startTime: Long, endTime: Long, results: List[Try[SearchResult]]): TraceCounts = {
    val traceCountsBuilder = TraceCounts.newBuilder()

    val searchResults: List[SearchResult] = results.map(searchResult => searchResult.get)

    val buckets: List[HistogramAggregation.Histogram] = searchResults.flatMap(result => {
      result.getAggregations.getHistogramAggregation(TraceCountsQueryGenerator.COUNT_HISTOGRAM_NAME)
        .getBuckets.asScala
        .filter(bucket => startTime <= bucket.getKey && bucket.getKey <= endTime)
    })

    buckets.groupBy(_.getKey).foreach(timeStampToHistogramMap => {
      val bucketCount = timeStampToHistogramMap._2.foldLeft(0L)((s, bucket) => s + bucket.getCount)
      val traceCount = TraceCount.newBuilder().setCount(bucketCount).setTimestamp(timeStampToHistogramMap._1)
      traceCountsBuilder.addTraceCount(traceCount)
    })

    traceCountsBuilder.build()
  }

  protected def extractFieldValues(results: List[Try[SearchResult]], fieldName: String): List[String] = {

    val searchResults: List[SearchResult] = results.map(searchResult => searchResult.get)

    val aggregations =
      searchResults.map(result => {
        result.getJsonObject
          .getAsJsonObject(ES_FIELD_AGGREGATIONS)
          .getAsJsonObject(ES_NESTED_DOC_NAME)
          .getAsJsonObject(fieldName)
      })

    aggregations.flatMap(aggregation => {
      if (aggregation.has(ES_FIELD_BUCKETS)) {
        aggregation
          .getAsJsonArray(ES_FIELD_BUCKETS)
          .asScala
          .map(element => element.getAsJsonObject.get(ES_FIELD_KEY).getAsString)
          .toList
      }
      else {
        aggregation
          .getAsJsonObject(fieldName)
          .getAsJsonArray(ES_FIELD_BUCKETS)
          .asScala
          .map(element => element.getAsJsonObject.get(ES_FIELD_KEY).getAsString)
          .toList
      }
    }).distinct // de-dup results

  }

  protected def extractStringFieldFromSource(source: String, fieldName: String): String = {
    (parse(source) \ fieldName).extract[String]
  }

  protected def extractServiceMetadata(results: List[Try[SearchResult]]): Seq[String] = {
    val searchResults: List[SearchResult] = results.map(searchResult => searchResult.get)
    searchResults.flatMap(result => {
      result.getAggregations.getTermsAggregation("distinct_services").getBuckets.asScala.map(_.getKey)
    }).distinct
  }

  protected def extractOperationMetadataFromSource(results: List[Try[SearchResult]], fieldName: String): List[String] = {
    val searchResults: List[SearchResult] = results.map(searchResult => searchResult.get)
    // go through each hit and fetch field from service_metadata
    val operationMetadata: List[String] = searchResults.map(result => result.getSourceAsStringList)
      .filter(sourceList => sourceList != null && sourceList.size() > 0)
      .flatMap(sourceList => sourceList
        .asScala
        .map(source => extractStringFieldFromSource(source, fieldName))
        .filter(!_.isEmpty))
      .distinct // de-dup fieldValues

    if (operationMetadata.nonEmpty) operationMetadata else Nil
  }
}
