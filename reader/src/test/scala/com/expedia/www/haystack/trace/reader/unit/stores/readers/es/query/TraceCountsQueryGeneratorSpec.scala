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

import com.expedia.open.tracing.api.{Field, TraceCountsRequest}
import com.expedia.www.haystack.trace.commons.clients.es.document.TraceIndexDoc
import com.expedia.www.haystack.trace.commons.config.entities.WhitelistIndexFieldConfiguration
import com.expedia.www.haystack.trace.reader.stores.readers.es.query.{TraceCountsQueryGenerator}
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec

class TraceCountsQueryGeneratorSpec extends BaseUnitTestSpec {
  describe("TraceSearchQueryGenerator") {
    it("should generate valid search queries") {
      Given("a trace search request")
      val `type` = "spans"
      val serviceName = "svcName"
      val operationName = "opName"
      val endTie = System.currentTimeMillis() * 1000
      val startTime = endTie - (600 * 1000 * 1000)
      val request = TraceCountsRequest
        .newBuilder()
        .addFields(Field.newBuilder().setName(TraceIndexDoc.SERVICE_KEY_NAME).setValue(serviceName).build())
        .addFields(Field.newBuilder().setName(TraceIndexDoc.OPERATION_KEY_NAME).setValue(operationName).build())
        .setStartTime(startTime)
        .setEndTime(endTie)
        .setInterval(60 * 1000 * 1000)
        .build()
      val queryGenerator = new TraceCountsQueryGenerator("haystack", `type`, "spans", new WhitelistIndexFieldConfiguration)

      When("generating query")
      val query = queryGenerator.generate(request, startTime)

      Then("generate a valid query")
      query.getURI.isEmpty should be(false)
    }
  }
}
