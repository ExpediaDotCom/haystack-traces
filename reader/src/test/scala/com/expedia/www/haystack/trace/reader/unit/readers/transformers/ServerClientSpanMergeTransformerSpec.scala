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


import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trace.reader.readers.transformers.ServerClientSpanMergeTransformer
import com.expedia.www.haystack.trace.reader.readers.utils.{AuxiliaryTags, MutableSpanForest, SpanMarkers}
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec
import com.expedia.www.haystack.trace.reader.unit.readers.builders.ValidTraceBuilder

import scala.collection.JavaConverters._

class ServerClientSpanMergeTransformerSpec extends BaseUnitTestSpec with ValidTraceBuilder {

  private def createSpansWithClientAndServer() = {
    val traceId = "traceId"

    val timestamp = System.currentTimeMillis() * 1000

    val serverSpanA = Span.newBuilder()
      .setSpanId("sa")
      .setTraceId(traceId)
      .setServiceName("aSvc")
      .setStartTime(timestamp + 100)
      .setDuration(1000)
      .build()

    val clientSpanA = Span.newBuilder()
      .setSpanId("ca")
      .setParentSpanId("sa")
      .setTraceId(traceId)
      .setServiceName("aSvc")
      .setStartTime(timestamp + 100)
      .setDuration(1000)
      .build()

    val serverSpanB = Span.newBuilder()
      .setSpanId("sb")
      .setParentSpanId("ca")
      .setServiceName("bSvc")
      .setTraceId(traceId)
      .setStartTime(timestamp + 200)
      .setDuration(100)
      .build()

    val clientSpanB_1 = Span.newBuilder()
      .setSpanId("cb1")
      .setParentSpanId("sb")
      .setServiceName("bSvc")
      .setTraceId(traceId)
      .setStartTime(timestamp + 300)
      .setDuration(100)
      .build()

    val clientSpanB_2 = Span.newBuilder()
      .setSpanId("cb2")
      .setParentSpanId("sb")
      .setServiceName("bSvc")
      .setStartTime(timestamp + 400)
      .setTraceId(traceId)
      .setDuration(100)
      .build()

    val serverSpanC_1 = Span.newBuilder()
      .setSpanId("sc1")
      .setParentSpanId("cb1")
      .setServiceName("cSvc")
      .setTraceId(traceId)
      .setStartTime(timestamp + 500)
      .setDuration(100)
      .build()

    val serverSpanC_2 = Span.newBuilder()
      .setSpanId("sc2")
      .setParentSpanId("cb2")
      .setServiceName("cSvc")
      .setTraceId(traceId)
      .setStartTime(timestamp + 600)
      .setDuration(100)
      .build()

    List(serverSpanA, clientSpanA, serverSpanB, clientSpanB_1, clientSpanB_2, serverSpanC_1, serverSpanC_2)
  }

  describe("ServerClientSpanMergeTransformer") {
    it("should merge the server client spans") {
      Given("a sequence of spans of a given trace")
      val spans = createSpansWithClientAndServer()

      When("invoking transform")
      val mergedSpans =
        new ServerClientSpanMergeTransformer().transform(MutableSpanForest(spans))

      val underlyingSpans = mergedSpans.getUnderlyingSpans

      Then("return partial spans merged with server span being primary")
      underlyingSpans.length should be(4)
      underlyingSpans.foreach(span => span.getTraceId shouldBe traceId)
      underlyingSpans.head.getSpanId shouldBe "sa"
      underlyingSpans.head.getParentSpanId shouldBe ""

      underlyingSpans.apply(1).getSpanId shouldBe "sb"
      underlyingSpans.apply(1).getParentSpanId shouldBe "sa"
      underlyingSpans.apply(1).getServiceName shouldBe "bSvc"
      underlyingSpans.apply(1).getTagsList.asScala.find(tag => tag.getKey.equals(AuxiliaryTags.IS_MERGED_SPAN)).get.getVBool shouldBe true
      underlyingSpans.apply(1).getLogsCount shouldBe 4

      underlyingSpans.apply(2).getSpanId shouldBe "sc1"
      underlyingSpans.apply(2).getParentSpanId shouldBe "sb"
      underlyingSpans.apply(2).getServiceName shouldBe "cSvc"
      underlyingSpans.apply(2).getTagsList.asScala.find(tag => tag.getKey.equals(AuxiliaryTags.IS_MERGED_SPAN)).get.getVBool shouldBe true
      underlyingSpans.apply(2).getLogsCount shouldBe 4

      underlyingSpans.apply(3).getSpanId shouldBe "sc2"
      underlyingSpans.apply(3).getParentSpanId shouldBe "sb"
      underlyingSpans.apply(3).getServiceName shouldBe "cSvc"
      underlyingSpans.apply(3).getTagsList.asScala.find(tag => tag.getKey.equals(AuxiliaryTags.IS_MERGED_SPAN)).get.getVBool shouldBe true
      underlyingSpans.apply(3).getLogsCount shouldBe 4

      mergedSpans.countTrees shouldBe 1
      val spanTree = mergedSpans.getAllTrees.head
      spanTree.span shouldBe underlyingSpans.head
      spanTree.children.size shouldBe 1
      spanTree.children.head.children.size shouldBe 2
      spanTree.children.head.span shouldBe underlyingSpans.apply(1)
      spanTree.children.head.children.map(_.span) should contain allOf(underlyingSpans.apply(2), underlyingSpans.apply(3))
      spanTree.children.head.children.foreach(tree => tree.children.size shouldBe 0)
    }
  }
}