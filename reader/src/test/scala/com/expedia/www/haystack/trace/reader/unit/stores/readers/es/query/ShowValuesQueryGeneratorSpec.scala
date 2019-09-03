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
package com.expedia.www.haystack.trace.reader.unit.stores.readers.es.query

import com.expedia.www.haystack.trace.reader.config.entities.ShowValuesIndexConfiguration
import com.expedia.www.haystack.trace.reader.stores.readers.es.query.ShowValuesQueryGenerator
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec
import com.google.gson.Gson

class ShowValuesQueryGeneratorSpec extends BaseUnitTestSpec {
  private val indexType = "fieldvalues-metadata"
  private val showValuesIndexConfiguration = ShowValuesIndexConfiguration(
    enabled = true,
    indexName = "show-values",
    indexType = indexType)

  describe("ShowValuesQueryGenerator") {
    it("should generate valid bool queries for field") {
      Given("a query generator")
      val queryGenerator = new ShowValuesQueryGenerator(showValuesIndexConfiguration)
      val serviceName = "test_service"
      val fieldName = "test_field"

      When("asked for aggregated service name")
      val query = queryGenerator.generateSearchFieldValuesQuery(serviceName, fieldName)

      Then("generate a valid query")
      query.getType should be(indexType)
      query.getData(new Gson()) shouldEqual "{\n  \"size\" : 10000,\n  \"query\" : {\n    \"bool\" : {\n      \"must\" : [\n        {\n          \"term\" : {\n            \"servicename\" : {\n              \"value\" : \"test_service\",\n              \"boost\" : 1.0\n            }\n          }\n        },\n        {\n          \"term\" : {\n            \"fieldname\" : {\n              \"value\" : \"test_field\",\n              \"boost\" : 1.0\n            }\n          }\n        }\n      ],\n      \"adjust_pure_negative\" : true,\n      \"boost\" : 1.0\n    }\n  },\n  \"_source\" : {\n    \"includes\" : [\n      \"fieldvalue\"\n    ],\n    \"excludes\" : [\n      \"fieldname\",\n      \"servicename\"\n    ]\n  }\n}"
      query.toString shouldEqual "Search{uri=show-values/fieldvalues-metadata/_search, method=POST}"
    }
  }
}
