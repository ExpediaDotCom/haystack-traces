/*
 *  Copyright 2019 Expedia, Inc.
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

package com.expedia.www.haystack.trace.storage.backends.mysql.integration

import java.util.UUID

import com.expedia.open.tracing.backend.{ReadSpansRequest, WriteSpansRequest}

class StorageBackendServiceIntegrationTestSpec extends BaseIntegrationTestSpec {


  describe("Mysql Persistence Service read trace records") {

    it("should read and write trace records for given traceID") {
      Given("trace")
      val traceId = UUID.randomUUID().toString
      val record = createTraceRecord(traceId)
      val writeSpansRequest = WriteSpansRequest.newBuilder().addRecords(record).build()

      When("writespans is invoked")
      val traceRecords = client.writeSpans(writeSpansRequest)

      Then("should write the trace")
      val readSpansRequest = ReadSpansRequest.newBuilder().addTraceIds(traceId).build()
      val retrievedRecord = client.readSpans(readSpansRequest)

      retrievedRecord.getRecordsList should not be empty
      retrievedRecord.getRecordsCount shouldEqual 1
      retrievedRecord.getRecordsList.get(0).getTraceId shouldEqual traceId
    }

  }
}
