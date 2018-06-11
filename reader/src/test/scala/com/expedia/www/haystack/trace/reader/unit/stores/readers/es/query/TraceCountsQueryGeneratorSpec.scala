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

import java.text.SimpleDateFormat
import java.util.Date

import com.expedia.open.tracing.api.{Field, TraceCountsRequest}
import com.expedia.www.haystack.trace.commons.clients.es.document.TraceIndexDoc
import com.expedia.www.haystack.trace.commons.config.entities.WhitelistIndexFieldConfiguration
import com.expedia.www.haystack.trace.reader.stores.readers.es.query.TraceCountsQueryGenerator
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec

class TraceCountsQueryGeneratorSpec extends BaseUnitTestSpec {
  val `type` = "spans"
  val ES_INDEX_HOUR_BUCKET = 6
  val ES_INDEX_HOUR_TTL = 72
  val INDEX_NAME_PREFIX = "haystack-spans"

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
      val queryGenerator = new TraceCountsQueryGenerator(INDEX_NAME_PREFIX, `type`, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL, "spans", new WhitelistIndexFieldConfiguration)

      When("generating query")
      val query = queryGenerator.generate(request, startTime)

      Then("generate a valid query")
      query.getURI.isEmpty should be(false)
    }

    it("should generate valid search queries for bucketed search count") {
      Given("a trace search request")
      val serviceName = "svcName"
      val operationName = "opName"
      val startTimeInMicros = 1
      val endTimeInMicros = 1527487220L * 1000 * 1000   // May 28, 2018 6:00:20 AM
      val bucketStartTime = endTimeInMicros - (4 * 3600 * 1000 * 1000L) // May 28, 2018 4:00:20 AM
      val interval = 60 * 1000 * 1000
      val request = TraceCountsRequest
        .newBuilder()
        .addFields(Field.newBuilder().setName(TraceIndexDoc.SERVICE_KEY_NAME).setValue(serviceName).build())
        .addFields(Field.newBuilder().setName(TraceIndexDoc.OPERATION_KEY_NAME).setValue(operationName).build())
        .setStartTime(startTimeInMicros)
        .setEndTime(endTimeInMicros)
        .setInterval(interval)
        .build()
      val queryGenerator = new TraceCountsQueryGenerator(INDEX_NAME_PREFIX, `type`, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL, "spans", new WhitelistIndexFieldConfiguration)

      When("generating query")
      val query = queryGenerator.generate(request, bucketStartTime)

      Then("generate a valid query")
      query.getURI.isEmpty should be(false)
      query.getURI.contains("haystack-spans-2018-05-28-0") should be(true)
    }

    it("should return a valid list of indexes for overlapping time range") {
      Given("starttime and endtime")
      val starttimeInMicros = 1527501725L * 1000 * 1000 // Monday, May 28, 2018 10:03:36 AM
      val endtimeInMicros = 1527512524L * 1000 * 1000   // Monday, May 28, 2018 1:02:04 PM
      val expectedIndexNames = getESIndexNames(starttimeInMicros, endtimeInMicros)
      val queryGenerator = new TraceCountsQueryGenerator(INDEX_NAME_PREFIX, `type`, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL, "spans", new WhitelistIndexFieldConfiguration)

      When("retrieving index names")
      val indexNames = queryGenerator.getESIndexes(starttimeInMicros, endtimeInMicros, INDEX_NAME_PREFIX, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL)

      Then("should get index names")
      indexNames should not be null
      indexNames.size shouldEqual expectedIndexNames.size
      indexNames.diff(expectedIndexNames).size should be(0)
    }

    it("should return a valid list of indexes") {
      Given("starttime and endtime")
      val starttimeInMicros = 1527487200L * 1000 * 1000 // May 28, 2018 6:00:00 AM
      val endtimeInMicros = 1527508800L * 1000 * 1000   // May 28, 2018 12:00:00 PM
      val expectedIndexNames = getESIndexNames(starttimeInMicros, endtimeInMicros)
      val queryGenerator = new TraceCountsQueryGenerator(INDEX_NAME_PREFIX, `type`, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL, "spans", new WhitelistIndexFieldConfiguration)

      When("retrieving index names")
      val indexNames = queryGenerator.getESIndexes(starttimeInMicros, endtimeInMicros, INDEX_NAME_PREFIX, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL)

      Then("should get index names")
      indexNames should not be null
      indexNames.size shouldEqual expectedIndexNames.size
      indexNames.diff(expectedIndexNames).size should be(0)
    }

    it("should return only a single index name for time range within same bucket") {
      Given("starttime and endtime")
      val starttimeInMicros = 1527487100L * 1000 * 1000 // May 28, 2018 5:58:20 AM
      val endtimeInMicros = 1527487120L * 1000 * 1000   // May 28, 2018 5:58:40 AM
      val expectedIndexNames = getESIndexNames(starttimeInMicros, endtimeInMicros)
      val queryGenerator = new TraceCountsQueryGenerator(INDEX_NAME_PREFIX, `type`, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL, "spans", new WhitelistIndexFieldConfiguration)

      When("retrieving index names")
      val indexNames = queryGenerator.getESIndexes(starttimeInMicros, endtimeInMicros, INDEX_NAME_PREFIX, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL)

      Then("should get index names")
      indexNames should not be null
      indexNames.size shouldEqual expectedIndexNames.size
      indexNames.diff(expectedIndexNames).size should be(0)
    }

    it("should return index alias (not return specific index) in case endtime minus starttime exceeds index retention") {
      Given("starttime and endtime")
      val starttimeInMicros = 0
      val endtimeInMicros = 1527487220L * 1000 * 1000   // May 28, 2018 6:00:20 AM
      val queryGenerator = new TraceCountsQueryGenerator(INDEX_NAME_PREFIX, `type`, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL, "spans", new WhitelistIndexFieldConfiguration)

      When("retrieving index names")
      val indexNames = queryGenerator.getESIndexes(starttimeInMicros, endtimeInMicros, INDEX_NAME_PREFIX, ES_INDEX_HOUR_BUCKET, ES_INDEX_HOUR_TTL)

      Then("should get index names")
      indexNames should not be null
      indexNames.size shouldEqual 1
      indexNames.head shouldEqual (INDEX_NAME_PREFIX)
    }
  }

  def getESIndexNames(starttimeInMicros: Long,
                      endtimeInMicros: Long): Seq[String] = {
    val INDEX_BUCKET_TIME_IN_MICROS = ES_INDEX_HOUR_BUCKET * 60 * 60 * 1000 * 1000L
    val flooredStarttime = starttimeInMicros - (starttimeInMicros % INDEX_BUCKET_TIME_IN_MICROS)
    val flooredEndtime = endtimeInMicros - (endtimeInMicros % INDEX_BUCKET_TIME_IN_MICROS)

    for (datetimeInMicros <- flooredStarttime to flooredEndtime by INDEX_BUCKET_TIME_IN_MICROS)
      yield {
        val date = new Date(datetimeInMicros / 1000)

        val dateBucket = new SimpleDateFormat("yyyy-MM-dd").format(date)
        val hourBucket = new SimpleDateFormat("HH").format(date).toInt / ES_INDEX_HOUR_BUCKET

        s"$INDEX_NAME_PREFIX-$dateBucket-$hourBucket"
      }
  }
}
