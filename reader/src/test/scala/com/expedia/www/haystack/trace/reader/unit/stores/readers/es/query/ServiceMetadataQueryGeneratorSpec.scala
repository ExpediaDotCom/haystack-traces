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

import com.expedia.www.haystack.trace.reader.config.entities.ServiceMetadataIndexConfiguration
import com.expedia.www.haystack.trace.reader.stores.readers.es.query.ServiceMetadataQueryGenerator
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec

class ServiceMetadataQueryGeneratorSpec extends BaseUnitTestSpec {
  private val indexType = "metadata"
  private val serviceMetadataIndexConfiguration = ServiceMetadataIndexConfiguration(
    enabled = true,
    indexName = "service_metadata",
    indexType = indexType)

  describe("ServiceMetadataQueryGenerator") {
    it("should generate valid aggregation queries for service names") {
      Given("a query generator")
      val queryGenerator = new ServiceMetadataQueryGenerator(serviceMetadataIndexConfiguration)

      When("asked for aggregated service name")
      val query = queryGenerator.generateQueryForServiceAggregations()

      Then("generate a valid query")
      query.getType should be(indexType)
    }

    it("should generate valid aggregation queries for operation names") {
      Given("a query generator and a service name")
      val queryGenerator = new ServiceMetadataQueryGenerator(serviceMetadataIndexConfiguration)
      val serviceName = "test_service"
      When("asked for aggregated operation names")
      val query = queryGenerator.generateQueryForOperationAggregations(serviceName)

      Then("generate a valid query")
      query.getType should be(indexType)
    }
  }
}
