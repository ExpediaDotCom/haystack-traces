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

import com.expedia.open.tracing.{Log, Span, Tag}
import com.expedia.www.haystack.trace.provider.providers.transformer.PartialSpanTransformer
import com.expedia.www.haystack.trace.provider.unit.BaseUnitTestSpec

class PartialSpanTransformerSpec extends BaseUnitTestSpec {

  def createSpansWithClientAndServer(timestamp: Long) = {
    val traceId = "traceId"
    val partialSpanId = "partialSpanId"
    val parentSpanId = "parentSpanId"
    val tag = Tag.newBuilder().setKey("tag").setVBool(true).build()
    val log = Log.newBuilder().setTimestamp(System.currentTimeMillis).addFields(tag).build()

    val partialClientSpan = Span.newBuilder()
      .setSpanId(partialSpanId)
      .setParentSpanId(parentSpanId)
      .setTraceId(traceId)
      .setStartTime(timestamp)
      .setDuration(1000)
      .addTags(tag)
      .addLogs(log)
      .build()

    val partialServerSpan = Span.newBuilder()
      .setSpanId(partialSpanId)
      .setParentSpanId(parentSpanId)
      .setTraceId(traceId)
      .setStartTime(timestamp + 20)
      .setDuration(980)
      .addTags(tag)
      .addLogs(log)
      .build()

    List(partialServerSpan, partialClientSpan)
  }

  describe("PartialSpanTransformer") {
    it("should merge partial spans") {
      Given("trace with partial spans")
      val timestamp = 150000000000l
      val spans = createSpansWithClientAndServer(timestamp)

      When("invoking transform")
      val mergedSpans = new PartialSpanTransformer().transform(spans)

      Then("return partial spans merged")
      mergedSpans.length should be(1)
      mergedSpans(0).getStartTime should be(timestamp)
      mergedSpans(0).getTagsCount should be(2)
      mergedSpans(0).getLogsCount should be(2)
    }
  }
}
