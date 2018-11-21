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
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.index.query.QueryBuilders.termQuery
import org.elasticsearch.search.builder.SearchSourceBuilder

class ServiceMetadataQueryGenerator(config: ServiceMetadataIndexConfiguration) {
  val SERVICE_NAME_KEY = "servicename"
  val OPERATION_NAME_KEY = "operationname"

  def generateSearchServiceQuery(): Search = {
    val serviceAggregationQuery = buildServiceAggregationQuery()
    generateSearchQuery(serviceAggregationQuery)
  }

  def generateSearchOperationQuery(serviceName: String): Search = {
    val serviceAggregationQuery = buildOperationAggregationQuery(serviceName)
    generateSearchQuery(serviceAggregationQuery)
  }

  private def generateSearchQuery(queryString: String): Search = {
    new Search.Builder(queryString)
      .addIndex(config.indexName)
      .addType(config.indexType)
      .build()
  }

  private def buildServiceAggregationQuery(): String = {
    new SearchSourceBuilder()
      .query(new MatchAllQueryBuilder())
      .fetchSource(SERVICE_NAME_KEY,OPERATION_NAME_KEY)
      .size(1000)
      .toString
  }

  private def buildOperationAggregationQuery(serviceName: String): String = {

    new SearchSourceBuilder()
      .query(termQuery(SERVICE_NAME_KEY, serviceName))
      .fetchSource(OPERATION_NAME_KEY,SERVICE_NAME_KEY)
      .size(1000)
      .toString
  }
}
