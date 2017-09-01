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
package com.expedia.www.haystack.trace.provider.unit.stores.readers.es.query

import com.expedia.open.tracing.internal.TracesSearchRequest
import com.expedia.www.haystack.trace.provider.stores.readers.es.query.TraceSearchQueryGenerator
import com.expedia.www.haystack.trace.provider.unit.BaseUnitTestSpec

class TraceSearchQueryGeneratorSpec extends BaseUnitTestSpec {
  describe("TraceSearchQueryGenerator") {
    it("should generate valid search queries") {
      Given("a trace search request")
      val `type` = "spans"
      val request = TracesSearchRequest
        .newBuilder()
        .setServiceName("svcName")
        .setOperationName("opName")
        .build()
      val queryGenerator = new TraceSearchQueryGenerator("haystack", `type`)

      When("generating query")
      val query = queryGenerator.generate(request)

      Then("generate a valid query")
      query.getType should be(`type`)
    }
  }
}
