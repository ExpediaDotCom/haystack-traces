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
import com.expedia.www.haystack.trace.reader.config.entities.ElasticSearchConfiguration
import com.expedia.www.haystack.trace.reader.stores.readers.es.query.TraceCountsQueryGenerator
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec
import com.google.gson.Gson

class TraceCountsQueryGeneratorSpec extends BaseUnitTestSpec {
  val ES_INDEX_HOUR_BUCKET = 6
  val ES_INDEX_HOUR_TTL = 72
  val INDEX_NAME_PREFIX = "haystack-spans"
  private val esConfig = ElasticSearchConfiguration("endpoint", None, None, INDEX_NAME_PREFIX, "spans", 5000, 5000, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL, true)

  describe("TraceSearchQueryGenerator") {
    it("should generate valid search queries") {
      Given("a trace search request")
      val serviceName = "svcName"
      val operationName = "opName"
      val startTime = 1529418475791000l // Tuesday, June 19, 2018 2:27:55.791 PM
      val endTime = 1529419075791000l // Tuesday, June 19, 2018 2:37:55.791 PM
      val request = TraceCountsRequest
        .newBuilder()
        .addFields(Field.newBuilder().setName(TraceIndexDoc.SERVICE_KEY_NAME).setValue(serviceName).build())
        .addFields(Field.newBuilder().setName(TraceIndexDoc.OPERATION_KEY_NAME).setValue(operationName).build())
        .setStartTime(startTime)
        .setEndTime(endTime)
        .setInterval(60 * 1000 * 1000)
        .build()
      val queryGenerator = new TraceCountsQueryGenerator(esConfig, "spans", new WhitelistIndexFieldConfiguration)

      When("generating query")
      val query = queryGenerator.generate(request)
      Then("generate a valid query")
      query.getData(new Gson()).replaceAll("\n", "").replaceAll(" ", "") shouldEqual "{\"size\":0,\"query\":{\"bool\":{\"filter\":[{\"nested\":{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"spans.servicename\":{\"value\":\"svcName\",\"boost\":1.0}}},{\"term\":{\"spans.operationname\":{\"value\":\"opName\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"path\":\"spans\",\"ignore_unmapped\":false,\"score_mode\":\"none\",\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"aggregations\":{\"countagg\":{\"histogram\":{\"field\":\"starttime\",\"interval\":6.0E7,\"offset\":0.0,\"order\":{\"_key\":\"asc\"},\"keyed\":false,\"min_doc_count\":0,\"extended_bounds\":{\"min\":1.529418475791E15,\"max\":1.529419075791E15}}}}}"
      query.getURI shouldEqual "haystack-spans-2018-06-19-2/spans/_search"
    }

    it("should generate valid search queries for bucketed search count") {
      Given("a trace search request")
      val serviceName = "svcName"
      val operationName = "opName"
      val startTimeInMicros = 1
      val endTimeInMicros = 1527487220L * 1000 * 1000   // May 28, 2018 6:00:20 AM
      val interval = 60 * 1000 * 1000
      val request = TraceCountsRequest
        .newBuilder()
        .addFields(Field.newBuilder().setName(TraceIndexDoc.SERVICE_KEY_NAME).setValue(serviceName).build())
        .addFields(Field.newBuilder().setName(TraceIndexDoc.OPERATION_KEY_NAME).setValue(operationName).build())
        .setStartTime(startTimeInMicros)
        .setEndTime(endTimeInMicros)
        .setInterval(interval)
        .build()
      val queryGenerator = new TraceCountsQueryGenerator(esConfig, "spans", new WhitelistIndexFieldConfiguration)

      When("generating query")
      val query = queryGenerator.generate(request)

      Then("generate a valid query")
      query.getURI shouldEqual "haystack-spans/spans/_search"
    }

    it("should return a valid list of indexes for overlapping time range") {
      Given("starttime and endtime")
      val startTimeInMicros = 1527501725L * 1000 * 1000 // Monday, May 28, 2018 10:03:36 AM
      val endTimeInMicros = 1527512524L * 1000 * 1000   // Monday, May 28, 2018 1:02:04 PM
      val queryGenerator = new TraceCountsQueryGenerator(esConfig, "spans", new WhitelistIndexFieldConfiguration)

      When("retrieving index names")
      val indexNames = queryGenerator.getESIndexes(startTimeInMicros, endTimeInMicros, INDEX_NAME_PREFIX, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL)

      Then("should get index names")
      indexNames should not be null
      indexNames.size shouldEqual 2
      indexNames should contain allOf("haystack-spans-2018-05-28-1", "haystack-spans-2018-05-28-2")
    }

    it("should return a valid list of indexes") {
      Given("starttime and endtime")
      val startTimeInMicros = 1527487200L * 1000 * 1000 // May 28, 2018 6:00:00 AM
      val endTimeInMicros = 1527508800L * 1000 * 1000   // May 28, 2018 12:00:00 PM
      val queryGenerator = new TraceCountsQueryGenerator(esConfig, "spans", new WhitelistIndexFieldConfiguration)

      When("retrieving index names")
      val indexNames = queryGenerator.getESIndexes(startTimeInMicros, endTimeInMicros, INDEX_NAME_PREFIX, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL)

      Then("should get index names")
      indexNames should not be null
      indexNames.size shouldEqual 2
      indexNames should contain allOf("haystack-spans-2018-05-28-1", "haystack-spans-2018-05-28-2")
    }

    it("should return only a single index name for time range within same bucket") {
      Given("starttime and endtime")
      val starttimeInMicros = 1527487100L * 1000 * 1000 // May 28, 2018 5:58:20 AM
      val endtimeInMicros = 1527487120L * 1000 * 1000   // May 28, 2018 5:58:40 AM
      val queryGenerator = new TraceCountsQueryGenerator(esConfig, "spans", new WhitelistIndexFieldConfiguration)

      When("retrieving index names")
      val indexNames = queryGenerator.getESIndexes(starttimeInMicros, endtimeInMicros, INDEX_NAME_PREFIX, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL)

      Then("should get index names")
      indexNames should not be null
      indexNames.size shouldBe 1
      indexNames.head shouldEqual "haystack-spans-2018-05-28-0"
    }

    it("should return index alias (not return specific index) in case endtime minus starttime exceeds index retention") {
      Given("starttime and endtime")
      val startTimeInMicros = 0
      val endTimeInMicros = 1527487220L * 1000 * 1000   // May 28, 2018 6:00:20 AM
      val queryGenerator = new TraceCountsQueryGenerator(esConfig, "spans", new WhitelistIndexFieldConfiguration)

      When("retrieving index names")
      val indexNames = queryGenerator.getESIndexes(startTimeInMicros, endTimeInMicros, INDEX_NAME_PREFIX, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL)

      Then("should get index names")
      indexNames should not be null
      indexNames.size shouldEqual 1
      indexNames.head shouldEqual INDEX_NAME_PREFIX
    }
  }
}
