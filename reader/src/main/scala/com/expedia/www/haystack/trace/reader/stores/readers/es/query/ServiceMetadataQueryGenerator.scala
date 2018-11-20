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

import com.expedia.www.haystack.trace.reader.config.entities.ServiceMetadataIndexConfiguration
import io.searchbox.core.Search
import org.elasticsearch.index.query.QueryBuilders.termQuery
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.elasticsearch.search.aggregations.support.ValueType
import org.elasticsearch.search.builder.SearchSourceBuilder

class ServiceMetadataQueryGenerator(config: ServiceMetadataIndexConfiguration) {


  def generateQueryForServiceAggregations(): Search = {
    val serviceAggregationQuery = buildServiceAggregationQuery()
    generateSearchQuery(serviceAggregationQuery)
  }

  def generateQueryForOperationAggregations(serviceName: String): Search = {
    val serviceAggregationQuery = buildOperationAggregationQuery(serviceName)
    generateSearchQuery(serviceAggregationQuery)
  }

  private def generateSearchQuery(queryString: String): Search = {
    new Search.Builder(queryString)
      .addIndex(config.indexName)
      .addType(config.indexType)
      .build()
  }

  //TODO ADD the service aggregation query
  private def buildServiceAggregationQuery(): String = {
    val fieldName = "servicename"
    val termsAggregationBuilder = new TermsAggregationBuilder(fieldName, ValueType.STRING)
      .size(1000)
    new SearchSourceBuilder()
      .aggregation(termsAggregationBuilder)
      .size(0)
      .toString
  }

  //TODO ADD the operation aggregation query
  private def buildOperationAggregationQuery(serviceName: String): String = {
    val serviceNameKey = "servicename"
    val operationNameKey = "operationname"
    val filterAggregationBuilder = new FilterAggregationBuilder(s"$serviceNameKey", termQuery(serviceNameKey, serviceName))
      .subAggregation(new TermsAggregationBuilder(s"$operationNameKey", ValueType.STRING)
        .size(1000))
    new SearchSourceBuilder()
      .aggregation(filterAggregationBuilder)
      .size(0)
      .toString
  }


}
