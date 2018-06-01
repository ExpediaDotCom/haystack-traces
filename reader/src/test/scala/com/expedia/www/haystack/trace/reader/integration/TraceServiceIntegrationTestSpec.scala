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

package com.expedia.www.haystack.trace.reader.integration

import java.util.UUID

import com.expedia.open.tracing.api._
import com.expedia.www.haystack.trace.commons.clients.es.document.TraceIndexDoc
import io.grpc.{Status, StatusRuntimeException}

import scala.collection.JavaConverters._

class TraceServiceIntegrationTestSpec extends BaseIntegrationTestSpec {

  describe("TraceReader.getTraceCounts") {
    it("should return trace counts histogram for given time span") {
      Given("traces elasticsearch")
      val serviceName = "dummy-servicename"
      val operationName = "dummy-operationname"
      val currrentTimeMicros: Long = System.currentTimeMillis() * 1000
      val randomStartTimes: Seq[String] =
        Seq(currrentTimeMicros.toString,
          (currrentTimeMicros - (10 * 1000 * 1000)).toString,
          (currrentTimeMicros - (20 * 1000 * 1000)).toString,
          (currrentTimeMicros - (30 * 1000 * 1000)).toString)
      val startTimeInMicroSec: Long = currrentTimeMicros - (randomStartTimes.size * 10 * 1000 * 1000)
      val endTimeInMicroSec: Long = currrentTimeMicros
      val intervalInMicroSec = endTimeInMicroSec - startTimeInMicroSec

      System.out.println("------- before ES calls ----------")
      putTracesForTimeline(serviceName, operationName, randomStartTimes)
      System.out.println("------- after ES calls ----------")

      val traceCountsRequest = TraceCountsRequest
        .newBuilder()
        .addFields(Field.newBuilder().setName(TraceIndexDoc.SERVICE_KEY_NAME).setValue(serviceName).build())
        .addFields(Field.newBuilder().setName(TraceIndexDoc.OPERATION_KEY_NAME).setValue(operationName).build())
        .setStartTime(1)
        .setEndTime(endTimeInMicroSec)
        .setInterval(intervalInMicroSec)
        .build()

      When("calling getTraceCounts")
      val traceCounts = client.getTraceCounts(traceCountsRequest)

      Then("should return possible values for given field")
      traceCounts should not be None
      System.out.println("------- traceCounts result size ----------" + traceCounts.toString)
      traceCounts.getTraceCountCount shouldEqual 1
      traceCounts.getTraceCount(0).getCount shouldEqual(randomStartTimes.size)
    }

    /* it("should return trace counts histogram for given time span") {
      Given("traces elasticsearch")
      val serviceName = "dummy-servicename"
      val operationName = "dummy-operationname"
      val endTimeInMicroSec: Long = System.currentTimeMillis() * 1000
      val startTimeInMicroSec: Long = endTimeInMicroSec - (900 * 1000 * 1000) // minus 15 mins
      val intervalInMicroSec = 60 * 1000 * 1000 // 1 min
      val spansInEachInterval = 5

      System.out.println("------- before ES calls ----------")
      insertTracesInEsForTimeline(serviceName, operationName, Map(), startTimeInMicroSec, endTimeInMicroSec, intervalInMicroSec, spansInEachInterval)
      insertTracesInEsForTimeline(serviceName, "some-other-oper", Map(), startTimeInMicroSec, endTimeInMicroSec, intervalInMicroSec, spansInEachInterval)
      System.out.println("------- after ES calls ----------")

      val traceCountsRequest = TraceCountsRequest
        .newBuilder()
        .addFields(Field.newBuilder().setName(TraceIndexDoc.SERVICE_KEY_NAME).setValue(serviceName).build())
        .addFields(Field.newBuilder().setName(TraceIndexDoc.OPERATION_KEY_NAME).setValue(operationName).build())
        .setStartTime(1)
        .setEndTime(endTimeInMicroSec)
        .setInterval(intervalInMicroSec)
        .build()

      When("calling getTraceCounts")
      val traceCounts = client.getTraceCounts(traceCountsRequest)

      Then("should return possible values for given field")
      traceCounts should not be None
      System.out.println("------- traceCounts result size ----------" + traceCounts.toString)
      val bucketCount = getBucketCount(startTimeInMicroSec, endTimeInMicroSec, intervalInMicroSec)
      traceCounts.getTraceCountCount shouldEqual bucketCount
      for (i <- 1 until bucketCount) {
        traceCounts.getTraceCountList.asScala.map(
          traceCount => {
            if (i == bucketCount) {
              traceCount.getTimestamp shouldEqual endTimeInMicroSec
            } else {
              traceCount.getTimestamp shouldEqual (startTimeInMicroSec + (intervalInMicroSec * i))
            }
            traceCount.getCount shouldEqual spansInEachInterval
          }
        )
      }
    }
  }*/
  }
}
