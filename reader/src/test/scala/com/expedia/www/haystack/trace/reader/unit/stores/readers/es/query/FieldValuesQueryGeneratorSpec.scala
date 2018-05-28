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

import com.expedia.open.tracing.api.{Field, FieldValuesRequest}
import com.expedia.www.haystack.trace.commons.config.entities.WhitelistIndexFieldConfiguration
import com.expedia.www.haystack.trace.reader.stores.readers.es.query.FieldValuesQueryGenerator
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec
import com.google.gson.Gson

class FieldValuesQueryGeneratorSpec extends BaseUnitTestSpec {
  describe("FieldValuesQueryGenerator") {
    it("should generate valid search queries") {
      Given("a trace search request")
      val `type` = "spans"
      val serviceName = "cloud-gate-proxy"
      val tagName = "tagName"
      val request = FieldValuesRequest
        .newBuilder()
        .setFieldName("operationName")
        .addFilters(Field.newBuilder().setName("serviceName").setValue(serviceName).build())
        .build()
      val queryGenerator = new FieldValuesQueryGenerator("haystack", `type`, "spans", new WhitelistIndexFieldConfiguration)

      When("generating query")
      val query = queryGenerator.generate(request)

      Then("generate a valid query")
      query.getType should be(`type`)
    }

    it("should generate caption independent search queries") {
      Given("a trace search request")
      val `type` = "spans"
      val serviceField = "serviceName"
      val operationField = "operationName"
      val serviceName = "svcName"
      val request = FieldValuesRequest
        .newBuilder()
        .setFieldName(operationField)
        .addFilters(Field.newBuilder().setName(serviceField).setValue(serviceName).build())
        .build()
      val queryGenerator = new FieldValuesQueryGenerator("haystack", `type`, "spans", new WhitelistIndexFieldConfiguration)

      When("generating query")
      val query = queryGenerator.generate(request)

      Then("generate a valid query with fields in lowercase")
      val queryString = query.getData(new Gson())
      queryString.contains(serviceField.toLowerCase()) should be(true)
      queryString.contains(operationField.toLowerCase()) should be(true)
    }
  }
}
