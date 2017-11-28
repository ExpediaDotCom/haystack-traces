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

package com.expedia.www.haystack.trace.reader.unit.readers.validators

import com.expedia.open.tracing.Span
import com.expedia.open.tracing.api.Trace
import com.expedia.www.haystack.trace.reader.exceptions.InvalidTraceException
import com.expedia.www.haystack.trace.reader.readers.validators.SingleRootValidator
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec

class SingleRootValidatorSpec extends BaseUnitTestSpec {
  val TRACE_ID = "traceId"

  describe("TraceIdValidator") {
    it("should throw exception for traces with multiple spans as root") {
      Given("trace with empty traceId")
      val trace = Trace.newBuilder()
        .setTraceId("traceId")
        .addChildSpans(Span.newBuilder().setTraceId(TRACE_ID).setSpanId("a"))
        .addChildSpans(Span.newBuilder().setTraceId(TRACE_ID).setSpanId("b"))
        .build()

      When("on validate")
      val validationResult = new SingleRootValidator().validate(trace)

      Then("throw InvalidTraceException")
      val thrown = the[InvalidTraceException] thrownBy validationResult.get
      thrown.getStatus.getDescription shouldEqual "Invalid Trace: found 2 roots with spanIDs=a,b and traceID=traceId"
    }

    it("should accept valid traces") {
      Given("trace with valid spans")
      val trace = Trace.newBuilder()
        .setTraceId(TRACE_ID)
        .addChildSpans(Span.newBuilder().setTraceId(TRACE_ID).setSpanId("a"))
        .addChildSpans(Span.newBuilder().setTraceId(TRACE_ID).setSpanId("b").setParentSpanId("a"))
        .addChildSpans(Span.newBuilder().setTraceId(TRACE_ID).setSpanId("c").setParentSpanId("a"))
        .build()

      When("on validate")
      val validationResult = new SingleRootValidator().validate(trace)

      Then("accept trace")
      noException should be thrownBy validationResult.get
    }
  }
}
