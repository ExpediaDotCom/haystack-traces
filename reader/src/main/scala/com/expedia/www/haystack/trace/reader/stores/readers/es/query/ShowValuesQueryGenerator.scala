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


import com.expedia.www.haystack.trace.reader.config.entities.ShowValuesIndexConfiguration
import io.searchbox.core.Search
import org.apache.commons.lang3.StringUtils
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.QueryBuilders.termQuery
import org.elasticsearch.search.builder.SearchSourceBuilder

class ShowValuesQueryGenerator(config: ShowValuesIndexConfiguration) {
  private val SERVICE_NAME_KEY = "servicename"
  private val FIELD_NAME_KEY = "fieldname"
  private val FIELD_VALUE_KEY = "fieldvalue"
  private val LIMIT = 10000


  def generateSearchFieldValuesQuery(serviceName: String, fieldName: String): Search = {
    val fieldValuesQuery = buildFieldValuesQuery(serviceName, fieldName)
    generateSearchQuery(fieldValuesQuery)
  }

  private def generateSearchQuery(queryString: String): Search = {
    new Search.Builder(queryString)
      .addIndex(config.indexName)
      .addType(config.indexType)
      .build()
  }

  private def buildFieldValuesQuery(serviceName: String, fieldName: String): String = {
    if (StringUtils.isNotEmpty(serviceName)) {
      new SearchSourceBuilder()
        .query(QueryBuilders
          .boolQuery()
          .must(termQuery(SERVICE_NAME_KEY, serviceName))
          .must(termQuery(FIELD_NAME_KEY, fieldName)))
        .fetchSource(Array[String](FIELD_VALUE_KEY), Array[String](FIELD_NAME_KEY, SERVICE_NAME_KEY))
        .size(LIMIT)
        .toString
    } else {
      new SearchSourceBuilder()
        .query(termQuery(FIELD_NAME_KEY, fieldName))
        .fetchSource(Array[String](FIELD_VALUE_KEY), Array[String](FIELD_NAME_KEY, SERVICE_NAME_KEY))
        .size(LIMIT)
        .toString
    }
  }
}
