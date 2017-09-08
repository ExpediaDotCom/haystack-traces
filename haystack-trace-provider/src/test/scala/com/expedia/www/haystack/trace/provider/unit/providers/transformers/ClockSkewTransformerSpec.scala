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

package com.expedia.www.haystack.trace.provider.unit.providers.transformers

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trace.provider.providers.transformers.ClockSkewTransformer
import com.expedia.www.haystack.trace.provider.unit.BaseUnitTestSpec

class ClockSkewTransformerSpec extends BaseUnitTestSpec {

  def createSpansWithSkew(timestamp: Long) = {
    // creating a trace with this timeline structure-
    // a -> b(-50)  -> e(-100) -> e'(-10)
    //              -> f(0)
    //   -> c(+500) -> g(+200)
    //   -> d(-100) -> h(0) -> i(+100)

    val traceId = "traceId"

    val spanA = Span.newBuilder()
      .setSpanId("a")
      .setTraceId(traceId)
      .setStartTime(timestamp)
      .setDuration(1000)
      .build()

    val spanB = Span.newBuilder()
      .setSpanId("b")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setStartTime(spanA.getStartTime - 50)
      .setDuration(100)
      .build()

    val spanC = Span.newBuilder()
      .setSpanId("c")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setStartTime(spanA.getStartTime + 500)
      .setDuration(100)
      .build()

    val spanD = Span.newBuilder()
      .setSpanId("d")
      .setParentSpanId("a")
      .setTraceId(traceId)
      .setStartTime(spanA.getStartTime - 100)
      .setDuration(100)
      .build()

    val spanE = Span.newBuilder()
      .setSpanId("e")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setStartTime(spanB.getStartTime - 100)
      .setDuration(100)
      .build()

    val spanF = Span.newBuilder()
      .setSpanId("f")
      .setParentSpanId("b")
      .setTraceId(traceId)
      .setStartTime(spanB.getStartTime)
      .setDuration(100)
      .build()

    val spanG = Span.newBuilder()
      .setSpanId("g")
      .setParentSpanId("c")
      .setTraceId(traceId)
      .setStartTime(spanC.getStartTime + 200)
      .setDuration(100)
      .build()

    val spanH = Span.newBuilder()
      .setSpanId("h")
      .setParentSpanId("d")
      .setTraceId(traceId)
      .setStartTime(spanD.getStartTime)
      .setDuration(100)
      .build()

    val spanI = Span.newBuilder()
      .setSpanId("i")
      .setParentSpanId("h")
      .setTraceId(traceId)
      .setStartTime(spanH.getStartTime + 100)
      .setDuration(100)
      .build()

    List(spanA, spanB, spanC, spanD, spanE, spanF, spanG, spanH, spanI)
  }

  describe("ClockSkewTransformer") {
    it("should fix clock skew") {
      Given("trace with skewed spans")
      val timestamp = 150000000000l
      val spans = createSpansWithSkew(timestamp)

      When("invoking transform")
      val transformedSpans = new ClockSkewTransformer().transform(spans)

      Then("return spans basedlined wrt parent spans")
      transformedSpans.length should be(9)
      transformedSpans.find(_.getSpanId == "a").get.getStartTime should be (timestamp)
      transformedSpans.find(_.getSpanId == "b").get.getStartTime should be (timestamp)
      transformedSpans.find(_.getSpanId == "c").get.getStartTime should be (timestamp + 500)
      transformedSpans.find(_.getSpanId == "d").get.getStartTime should be (timestamp)
      transformedSpans.find(_.getSpanId == "e").get.getStartTime should be (timestamp)
      transformedSpans.find(_.getSpanId == "f").get.getStartTime should be (timestamp)
      transformedSpans.find(_.getSpanId == "g").get.getStartTime should be (timestamp + 700)
      transformedSpans.find(_.getSpanId == "h").get.getStartTime should be (timestamp)
      transformedSpans.find(_.getSpanId == "i").get.getStartTime should be (timestamp + 100)
    }
  }
}
