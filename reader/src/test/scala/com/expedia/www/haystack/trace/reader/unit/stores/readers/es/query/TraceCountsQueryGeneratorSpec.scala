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
  val `type` = "spans"
  val ES_INDEX_HOUR_BUCKET = 6
  val ES_INDEX_HOUR_TTL = 72

  describe("TraceSearchQueryGenerator") {
    it("should generate valid search queries") {
      Given("a trace search request")
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
      val queryGenerator = new TraceCountsQueryGenerator("haystack-traces", `type`, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL, "spans", new WhitelistIndexFieldConfiguration)

      When("generating query")
      val query = queryGenerator.generate(request, startTime)

      Then("generate a valid query")
      query.getURI.isEmpty should be(false)
    }

    it("should return a valid list of indexes for overlapping time range") {
      Given("starttime and endtime")
      val starttimeInSecs = 1527501725L   // Monday, May 28, 2018 10:03:36 AM
      val endtimeInSecs = 1527512524L     // Monday, May 28, 2018 1:02:04 PM

      val queryGenerator = new TraceCountsQueryGenerator("haystack-traces", `type`, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL, "spans", new WhitelistIndexFieldConfiguration)

      When("retrieving index names")
      val indexNames = queryGenerator.getESIndexes(starttimeInSecs, endtimeInSecs, "haystack-traces", ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL)

      Then("should get index names")
      indexNames should not be null
      indexNames.size should be (2)
      indexNames(0) shouldEqual "haystack-traces-2018-05-28-0"
      indexNames(1) shouldEqual "haystack-traces-2018-05-28-1"
    }

    it("should return a valid list of indexes") {
      Given("starttime and endtime")
      val starttimeInSecs = 1527487200L   // Monday, May 28, 2018 6:00:00 AM
      val endtimeInSecs = 1527508800L     // Monday, May 28, 2018 12:00:00 PM

      val queryGenerator = new TraceCountsQueryGenerator("haystack-traces", `type`, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL, "spans", new WhitelistIndexFieldConfiguration)

      When("retrieving index names")
      val indexNames = queryGenerator.getESIndexes(starttimeInSecs, endtimeInSecs, "haystack-traces", ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL)

      Then("should get index names")
      indexNames should not be null
      indexNames.size should be (2)
      indexNames(0) shouldEqual "haystack-traces-2018-05-28-0"
      indexNames(1) shouldEqual "haystack-traces-2018-05-28-1"
    }

    it("should return only a single index name for time range within same bucket") {
      Given("starttime and endtime")
      val starttimeInSecs = 1527487210L   // Monday, May 28, 2018 6:00:10 AM
      val endtimeInSecs = 1527487220L     // Monday, May 28, 2018 6:00:20 AM

      val queryGenerator = new TraceCountsQueryGenerator("haystack-traces", `type`, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL, "spans", new WhitelistIndexFieldConfiguration)

      When("retrieving index names")
      val indexNames = queryGenerator.getESIndexes(starttimeInSecs, endtimeInSecs, "haystack-traces", ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL)

      Then("should get index names")
      indexNames should not be null
      indexNames.size should be (1)
      indexNames(0) shouldEqual "haystack-traces-2018-05-28-0"
    }

    it("should return index alias (not return specific index) in case endtime minus starttime exceeds index retention") {
      Given("starttime and endtime")
      val starttimeInSecs = 0
      val endtimeInSecs = 1527487220L     // Monday, May 28, 2018 6:00:20 AM

      val queryGenerator = new TraceCountsQueryGenerator("haystack-traces", `type`, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL, "spans", new WhitelistIndexFieldConfiguration)

      When("retrieving index names")
      val indexNames = queryGenerator.getESIndexes(starttimeInSecs, endtimeInSecs, "haystack-traces", ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL)

      Then("should get index names")
      indexNames should not be null
      indexNames.size should be (1)
      indexNames(0) shouldEqual "haystack-traces"
    }
  }
}
