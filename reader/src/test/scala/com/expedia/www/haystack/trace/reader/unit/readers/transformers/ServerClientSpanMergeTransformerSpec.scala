/*
 *  Copyright 2018 Expedia, Inc.
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

package com.expedia.www.haystack.trace.reader.unit.readers.transformers


import com.expedia.open.tracing.{Log, Span, Tag}
import com.expedia.www.haystack.trace.reader.readers.transformers.{PartialSpanTransformer, ServerClientSpanMergeTransformer}
import com.expedia.www.haystack.trace.reader.readers.utils.MutableSpanForest
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec
import com.expedia.www.haystack.trace.reader.unit.readers.builders.ValidTraceBuilder

class ServerClientSpanMergeTransformerSpec extends BaseUnitTestSpec with ValidTraceBuilder {

  private def createSpansWithClientAndServer() = {
    val traceId = "traceId"

    val serverSpanA = Span.newBuilder()
      .setSpanId("a1")
      .setParentSpanId("pa1")
      .setTraceId(traceId)
      .setServiceName("aSvc")
      .addLogs(Log.newBuilder()
        .setTimestamp(System.currentTimeMillis)
        .addFields(Tag.newBuilder().setKey("event").setVStr("sr").build())
        .build())
      .addLogs(Log.newBuilder()
        .setTimestamp(System.currentTimeMillis)
        .addFields(Tag.newBuilder().setKey("event").setVStr("ss").build())
        .build())
      .setDuration(1000)
      .build()

    val clientSpanA = Span.newBuilder()
      .setSpanId("a")
      .setParentSpanId("a1")
      .setTraceId(traceId)
      .setServiceName("aSvc")
      .addLogs(Log.newBuilder()
        .setTimestamp(System.currentTimeMillis)
        .addFields(Tag.newBuilder().setKey("event").setVStr("cr").build())
        .build())
      .addLogs(Log.newBuilder()
        .setTimestamp(System.currentTimeMillis)
        .addFields(Tag.newBuilder().setKey("event").setVStr("cs").build())
        .build())
      .setDuration(1000)
      .build()

    val serverSpanB = Span.newBuilder()
      .setSpanId("a")
      .setParentSpanId("a1")
      .setServiceName("bSvc")
      .setTraceId(traceId)
      .addLogs(Log.newBuilder()
        .setTimestamp(System.currentTimeMillis)
        .addFields(Tag.newBuilder().setKey("event").setVStr("sr").build())
        .build())
      .addLogs(Log.newBuilder()
        .setTimestamp(System.currentTimeMillis)
        .addFields(Tag.newBuilder().setKey("event").setVStr("ss").build())
        .build())
      .setDuration(100)
      .build()

    val clientSpanB_1 = Span.newBuilder()
      .setSpanId("b1")
      .setParentSpanId("a")
      .setServiceName("bSvc")
      .setTraceId(traceId)
      .setDuration(100)
      .build()

    val clientSpanB_2 = Span.newBuilder()
      .setSpanId("b2")
      .setParentSpanId("a")
      .setServiceName("bSvc")
      .setTraceId(traceId)
      .setDuration(100)
      .build()

    val serverSpanC_1 = Span.newBuilder()
      .setSpanId("c1")
      .setParentSpanId("b1")
      .setServiceName("cSvc")
      .setTraceId(traceId)
      .setDuration(100)
      .build()

    val serverSpanC_2 = Span.newBuilder()
      .setSpanId("c2")
      .setParentSpanId("b2")
      .setServiceName("cSvc")
      .setTraceId(traceId)
      .setDuration(100)
      .build()

    List(serverSpanA, clientSpanA, serverSpanB, clientSpanB_1, clientSpanB_2, serverSpanC_1, serverSpanC_2)
  }

  describe("ServerClientSpanMergeTransformer") {
    it("should merge the server client spans") {
      Given("a sequence of spans of a given trace")
      val spans = createSpansWithClientAndServer()

      When("invoking transform")
      val mergedSpans = new ServerClientSpanMergeTransformer().transform(new PartialSpanTransformer().transform(MutableSpanForest(spans)))
      val underlyingSpans = mergedSpans.getUnderlyingSpans

      Then("return partial spans merged with server span being primary")
      underlyingSpans.length should be(4)
    }
  }
}