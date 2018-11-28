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

package com.expedia.www.haystack.trace.storage.backends.cassandra.integration

import java.util.UUID

import com.expedia.open.tracing.backend.ReadSpansRequest

class CassandraStorageBackendServiceIntegrationTestSpec extends BaseIntegrationTestSpec {


  describe("Cassandra Persistence Service read trace records") {
    it("should get trace records for given traceID from cassandra") {
      Given("trace in cassandra")
      val traceId = UUID.randomUUID().toString
      putTraceInCassandra(traceId)

      val readSpansRequest = ReadSpansRequest.newBuilder().addTraceIds(traceId).build()

      When("readspans is invoked")
      val traceRecords = client.readSpans(readSpansRequest)

      Then("should return the trace")
      traceRecords.getRecordsList should not be empty
    }
  }
}
