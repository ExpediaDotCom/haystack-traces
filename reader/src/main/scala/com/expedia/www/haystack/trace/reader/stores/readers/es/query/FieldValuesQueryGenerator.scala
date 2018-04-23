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

import java.util

import com.expedia.open.tracing.api.{Field, FieldValuesRequest}
import io.searchbox.core.Search
import io.searchbox.strings.StringUtils
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query._
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.elasticsearch.search.aggregations.support.ValueType
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.JavaConverters._

class FieldValuesQueryGenerator(indexNamePrefix: String, indexType: String, nestedDocName: String) {
  def generate(request: FieldValuesRequest): Search = {
    new Search.Builder(buildQueryString(request))
      .addIndex(s"$indexNamePrefix*")
      .addType(indexType)
      .build()
  }

  private def buildQueryString(request: FieldValuesRequest) =
    new SearchSourceBuilder()
      .aggregation(createNestedAggregationQuery(request.getFieldName.toLowerCase(), request.getFiltersList))
      .size(0)
      .toString

  private def createNestedAggregationQuery(fieldName: String, filters: util.List[Field]): AggregationBuilder = {
    val subAggregation =
      if(filters.size() == 0)
        new TermsAggregationBuilder(s"$fieldName-terms-agg", ValueType.STRING)
          .field(withBaseDoc(fieldName))
          .size(1000)
      else
        createQueryAggregation(fieldName, filters)

    new NestedAggregationBuilder(s"$nestedDocName-nested-agg", nestedDocName).subAggregation(subAggregation)
  }

  private def createQueryAggregation(fieldName: String, filters: util.List[Field]) = {
    val boolQueryBuilder = boolQuery()

    // add all fields as term sub query
    val subQueries: Seq[QueryBuilder] =
      for (field <- filters.asScala;
           termQuery = buildTermQuery(field.getName.toLowerCase, field.getValue); if termQuery.isDefined) yield termQuery.get
    subQueries.foreach(boolQueryBuilder.filter)

    // combined filter and aggregation query
    new FilterAggregationBuilder(s"$fieldName-filter-agg", boolQueryBuilder)
      .subAggregation(new TermsAggregationBuilder(s"$fieldName-terms-agg", ValueType.STRING)
        .field(withBaseDoc(fieldName))
        .size(1000))
  }

  private def buildTermQuery(key: String, value: String): Option[TermQueryBuilder] = {
    if (StringUtils.isBlank(value)) None else Some(termQuery(withBaseDoc(key), value))
  }

  private def withBaseDoc(field: String) = s"$nestedDocName.$field"
}
