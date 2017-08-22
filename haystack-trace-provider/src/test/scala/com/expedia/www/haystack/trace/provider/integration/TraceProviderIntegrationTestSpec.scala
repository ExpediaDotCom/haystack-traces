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

package com.expedia.www.haystack.trace.provider.integration

import java.util.UUID

import com.expedia.open.tracing.internal.{SpanRequest, TraceProviderGrpc, TraceRequest}
import io.grpc.{ManagedChannelBuilder, Status, StatusRuntimeException}

class TraceProviderIntegrationTestSpec extends BaseIntegrationTestSpec {
  val client = TraceProviderGrpc.newBlockingStub(ManagedChannelBuilder.forAddress("haystack-trace-provider", 8080)
    .usePlaintext(true)
    .build())

  describe("TraceProvider.getTrace") {
    it("should get trace for given traceID from cassandra") {
      Given("trace in cassandra")
      val traceId = UUID.randomUUID().toString
      putTraceInCassandra(traceId)

      When("getTrace is invoked")
      val trace = client.getTrace(TraceRequest.newBuilder().setTraceId(traceId).build())

      Then("should return the trace")
      trace.getTraceId shouldBe traceId
    }

    it("should return TraceNotFound exception if traceID is not in cassandra") {
      Given("trace in cassandra")
      putTraceInCassandra(UUID.randomUUID().toString)

      When("getTrace is invoked")
      val thrown = the[StatusRuntimeException] thrownBy {
        client.getTrace(TraceRequest.newBuilder().setTraceId(UUID.randomUUID().toString).build())
      }

      Then("thrown StatusRuntimeException should have 'not found' error")
      thrown.getStatus.getCode should be(Status.NOT_FOUND.getCode)
      thrown.getStatus.getDescription should include("traceId not found")
    }
  }

  describe("TraceProvider.getRawTrace") {
    it("should get trace for given traceID from cassandra") {
      Given("trace in cassandra")
      val traceId = UUID.randomUUID().toString
      putTraceInCassandra(traceId)

      When("getRawTrace is invoked")
      val trace = client.getRawTrace(TraceRequest.newBuilder().setTraceId(traceId).build())

      Then("should return the trace")
      trace.getTraceId shouldBe traceId
    }

    it("should return TraceNotFound exception if traceID is not in cassandra") {
      Given("trace in cassandra")
      putTraceInCassandra(UUID.randomUUID().toString)

      When("getRawTrace is invoked")
      val thrown = the[StatusRuntimeException] thrownBy {
        client.getRawTrace(TraceRequest.newBuilder().setTraceId(UUID.randomUUID().toString).build())
      }

      Then("thrown StatusRuntimeException should have 'not found' error")
      thrown.getStatus.getCode should be(Status.NOT_FOUND.getCode)
      thrown.getStatus.getDescription should include("traceId not found")
    }
  }

  describe("TraceProvider.getRawSpan") {
    it("should get spanId for given traceID-spanId from cassandra") {
      Given("trace in cassandra")
      val traceId = UUID.randomUUID().toString
      val spanId = UUID.randomUUID().toString
      putTraceInCassandra(traceId, spanId)

      When("getRawSpan is invoked")
      val span = client.getRawSpan(SpanRequest
        .newBuilder()
        .setTraceId(traceId)
        .setSpanId(spanId)
        .build())

      Then("should return the trace")
      span.getTraceId shouldBe traceId
      span.getSpanId shouldBe spanId
    }

    it("should return TraceNotFound exception if traceID is not in cassandra") {
      Given("trace in cassandra")
      putTraceInCassandra(UUID.randomUUID().toString)

      When("getRawSpan is invoked")
      val thrown = the[StatusRuntimeException] thrownBy {
        client.getRawSpan(SpanRequest
          .newBuilder()
          .setTraceId(UUID.randomUUID().toString)
          .setSpanId(UUID.randomUUID().toString)
          .build())
      }

      Then("thrown StatusRuntimeException should have 'traceId not found' error")
      thrown.getStatus.getCode should be(Status.NOT_FOUND.getCode)
      thrown.getStatus.getDescription should include("traceId not found")
    }

    it("should return SpanNotFound exception if spanId is not part of Trace") {
      Given("trace in cassandra")
      val traceId = UUID.randomUUID().toString
      putTraceInCassandra(traceId)

      When("getRawSpan is invoked")
      val thrown = the[StatusRuntimeException] thrownBy {
        client.getRawSpan(SpanRequest
          .newBuilder()
          .setTraceId(traceId)
          .setSpanId(UUID.randomUUID().toString)
          .build())
      }

      Then("thrown StatusRuntimeException should have 'spanId not found' error")
      thrown.getStatus.getCode should be(Status.NOT_FOUND.getCode)
      thrown.getStatus.getDescription should include("spanId not found")
    }
  }
}
