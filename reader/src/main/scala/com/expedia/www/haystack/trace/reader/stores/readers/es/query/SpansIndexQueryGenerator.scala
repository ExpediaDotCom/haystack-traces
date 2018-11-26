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

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.expedia.open.tracing.api.Operand.OperandCase
import com.expedia.open.tracing.api.{ExpressionTree, Field}
import com.expedia.www.haystack.trace.commons.config.entities.WhitelistIndexFieldConfiguration
import io.searchbox.strings.StringUtils
import org.apache.lucene.search.join.ScoreMode
import org.elasticsearch.index.query.QueryBuilders.{boolQuery, nestedQuery, termQuery}
import org.elasticsearch.index.query.{BoolQueryBuilder, NestedQueryBuilder, QueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.elasticsearch.search.aggregations.support.ValueType

import scala.collection.JavaConverters._

abstract class SpansIndexQueryGenerator(nestedDocName: String, indexConfiguration: WhitelistIndexFieldConfiguration) {
  private final val TIME_ZONE = TimeZone.getTimeZone("UTC")

  // create search query by using filters list
  protected def createFilterFieldBasedQuery(filterFields: java.util.List[Field]): BoolQueryBuilder = {
    val traceContextWhitelistFields = indexConfiguration.globalTraceContextIndexFieldNames
    val (traceContextFields, serviceContextFields) = filterFields
      .asScala
      .partition(f => traceContextWhitelistFields.contains(f.getName.toLowerCase))

    val query = boolQuery()
    createNestedQuery(serviceContextFields).map(query.filter)
    traceContextFields.foreach(f => {
      query.filter(createNestedQuery(Seq(f)).get)
    })

    query
  }

  // create search query by using filters expression tree
  protected def createExpressionTreeBasedQuery(expression: ExpressionTree): BoolQueryBuilder = {
    val perContextFields = toListOfFilters(expression)
    val query = boolQuery()

    // create a nested boolean query per
    perContextFields.foreach(fields => {
      query.filter(createNestedQuery(fields).get)
    })

    query
  }

  // create list of fields, one for each trace level query and one for each span level groups
  // assuming that first level is trace level filters
  // and second level are span level filter groups
  private def toListOfFilters(expression: ExpressionTree): List[List[Field]] = {
    val (spanLevel, traceLevel) = expression.getOperandsList.asScala.partition(operand => operand.getOperandCase == OperandCase.EXPRESSION)

    val traceLevelFilters = traceLevel.map(field => List(field.getField))
    val spanLevelFilters = spanLevel.map(tree => toListOfSpanLevelFilters(tree.getExpression))

    (spanLevelFilters ++ traceLevelFilters).toList
  }

  private def toListOfSpanLevelFilters(expression: ExpressionTree): List[Field] = {
    expression.getOperandsList.asScala.map(field => field.getField).toList
  }

  private def createNestedQuery(fields: Seq[Field]): Option[NestedQueryBuilder] = {
    if (fields.isEmpty) {
      None
    } else {
      val nestedBoolQueryBuilder = createBoolQuery(fields)
      Some(nestedQuery(nestedDocName, nestedBoolQueryBuilder, ScoreMode.None))
    }
  }

  private def buildTermQuery(key: String, value: String): Option[TermQueryBuilder] = {
    if (StringUtils.isBlank(value)) None else Some(termQuery(withBaseDoc(key), value))
  }

  protected def createBoolQuery(fields: Seq[Field]): BoolQueryBuilder = {
    val boolQueryBuilder = boolQuery()
    // add all fields as term sub query
    val subQueries: Seq[QueryBuilder] =
      for (field <- fields;
           termQuery = buildTermQuery(field.getName.toLowerCase, field.getValue); if termQuery.isDefined) yield termQuery.get
    subQueries.foreach(boolQueryBuilder.filter)

    boolQueryBuilder
  }

  protected def createNestedAggregationQuery(fieldName: String): AggregationBuilder =
    new NestedAggregationBuilder(nestedDocName, nestedDocName)
      .subAggregation(
        new TermsAggregationBuilder(fieldName, ValueType.STRING)
          .field(withBaseDoc(fieldName))
          .size(1000))

  protected def createNestedAggregationQueryWithNestedFilters(fieldName: String, filterFields: java.util.List[Field]): AggregationBuilder = {
    val boolQueryBuilder = createBoolQuery(filterFields.asScala)

    new NestedAggregationBuilder(nestedDocName, nestedDocName)
      .subAggregation(
        new FilterAggregationBuilder(s"$fieldName", boolQueryBuilder)
          .subAggregation(new TermsAggregationBuilder(s"$fieldName", ValueType.STRING)
            .field(withBaseDoc(fieldName))
            .size(1000))
      )
  }

  def getESIndexes(startTimeInMicros: Long,
                   endTimeInMicros: Long,
                   indexNamePrefix: String,
                   indexHourBucket: Int,
                   indexHourTtl: Int): Seq[String] = {

    if (!isValidTimeRange(startTimeInMicros, endTimeInMicros, indexHourTtl)) {
      Seq(s"$indexNamePrefix")
    } else {
      val INDEX_BUCKET_TIME_IN_MICROS: Long = indexHourBucket.toLong * 60 * 60 * 1000 * 1000
      val flooredStarttime = startTimeInMicros - (startTimeInMicros % INDEX_BUCKET_TIME_IN_MICROS)
      val flooredEndtime = endTimeInMicros - (endTimeInMicros % INDEX_BUCKET_TIME_IN_MICROS)

      for (datetimeInMicros <- flooredStarttime to flooredEndtime by INDEX_BUCKET_TIME_IN_MICROS)
        yield {
          val date = new Date(datetimeInMicros / 1000)
          val dateBucket = createSimpleDateFormat("yyyy-MM-dd").format(date)
          val hourBucket = createSimpleDateFormat("HH").format(date).toInt / indexHourBucket

          s"$indexNamePrefix-$dateBucket-$hourBucket"
        }
    }
  }

  private def createSimpleDateFormat(pattern: String): SimpleDateFormat = {
    val sdf = new SimpleDateFormat(pattern)
    sdf.setTimeZone(TIME_ZONE)
    sdf
  }

  private def isValidTimeRange(startTimeInMicros: Long,
                               endTimeInMicros: Long,
                               indexHourTtl: Int): Boolean = {
    (endTimeInMicros - startTimeInMicros) < (indexHourTtl.toLong * 60 * 60 * 1000 * 1000)
  }
  
  protected def withBaseDoc(field: String) = s"$nestedDocName.$field"
}
