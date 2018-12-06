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

package com.expedia.www.haystack.trace.reader.unit.readers.transformers

import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.trace.commons.utils.SpanUtils
import com.expedia.www.haystack.trace.reader.readers.transformers.InvalidRootTransformer
import com.expedia.www.haystack.trace.reader.readers.utils.MutableSpanForest
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec

import scala.collection.JavaConverters._

class InvalidRootTransformerSpec extends BaseUnitTestSpec {
  describe("InvalidRootTransformer") {
    it("should mark first span as root when there are multiple roots") {
      Given("trace with multiple roots ")
      val spanForest = MutableSpanForest(Seq(
        Span.newBuilder()
          .setSpanId("a")
          .setServiceName("sa")
          .setStartTime(150000000000l + 300)
          .build(),
        Span.newBuilder()
          .setSpanId("b")
          .setServiceName("sb")
          .setStartTime(150000000000l)
          .build(),
        Span.newBuilder()
          .setSpanId("c")
          .setServiceName("sc")
          .setStartTime(150000000000l + 150)
          .build()
      ))

      When("invoking transform")
      val transformedSpans = new InvalidRootTransformer().transform(spanForest).getUnderlyingSpans

      Then("pick first span as root and mark second's parent to be root")
      transformedSpans.length should be(4)

      val root = transformedSpans.filter(_.getParentSpanId.isEmpty)
      root.size should be(1)
      root.head.getServiceName shouldEqual "sb"
      root.head.getOperationName shouldEqual "auto-generated"
      root.head.getStartTime shouldBe 150000000000l
      root.head.getDuration shouldBe 300l

      val others = transformedSpans.filter(!_.getParentSpanId.isEmpty)
      others.foreach(span => span.getParentSpanId should be(root.head.getSpanId))
    }

    it("should not generate any autogenerated for a complete tree but without a parent span existing") {
      Given("trace with multiple roots ")
      val spanForest = MutableSpanForest(Seq(
        Span.newBuilder()
          .setSpanId("a")
          .setParentSpanId("b")
          .setServiceName("sa")
          .setStartTime(150000000000l + 300)
          .build(),
        Span.newBuilder()
          .setSpanId("b")
          .setParentSpanId("d")
          .setServiceName("sb")
          .setStartTime(150000000000l)
          .build(),
        Span.newBuilder()
          .setSpanId("c")
          .setParentSpanId("b")
          .setServiceName("sc")
          .setStartTime(150000000000l + 150)
          .build()
      ))

      When("invoking transform")
      val transformedSpans = new InvalidRootTransformer().transform(spanForest).getUnderlyingSpans

      Then("pick first span as root and mark second's parent to be root")
      transformedSpans.length should be(3)

      val root = transformedSpans.filter(_.getParentSpanId.isEmpty)
      root.size should be(1)
      root.head.getServiceName shouldEqual "sb"
      root.head.getStartTime shouldBe 150000000000l

      val others = transformedSpans.filter(!_.getParentSpanId.isEmpty)
      others.foreach(span => span.getParentSpanId should be(root.head.getSpanId))
    }

    it("should mark first span as root when there are no roots") {
      Given("trace with multiple roots ")
      val spanForest = MutableSpanForest(Seq(
        Span.newBuilder()
          .setSpanId("a")
          .setParentSpanId("x")
          .setStartTime(150000000000l + 300)
          .build(),
        Span.newBuilder()
          .setSpanId("b")
          .setParentSpanId("x")
          .setStartTime(150000000000l)
          .build(),
        Span.newBuilder()
          .setSpanId("c")
          .setParentSpanId("x")
          .setStartTime(150000000000l + 150)
          .build()
      ))

      When("invoking transform")
      val transformedSpans = new InvalidRootTransformer().transform(spanForest).getUnderlyingSpans

      Then("pick first span as root and mark second's parent to be root")
      transformedSpans.length should be(3)

      val root = transformedSpans.filter(_.getParentSpanId.isEmpty)
      root.size should be(1)
      root.head.getSpanId should be("b")
    }

    it("should mark loopback span as root when there are no roots") {
      Given("trace with multiple roots ")
      val spanForest = MutableSpanForest(Seq(
        Span.newBuilder()
          .setSpanId("a")
          .setParentSpanId("x")
          .setStartTime(150000000000l + 300)
          .build(),
        Span.newBuilder()
          .setSpanId("b")
          .setParentSpanId("x")
          .setStartTime(150000000000l)
          .build(),
        Span.newBuilder()
          .setSpanId("c")
          .setParentSpanId("c")
          .setStartTime(150000000000l + 150)
          .build()
      ))

      When("invoking transform")
      val transformedSpans = new InvalidRootTransformer().transform(spanForest).getUnderlyingSpans

      Then("pick first span as root and mark second's parent to be root")
      transformedSpans.length should be(3)

      val root = transformedSpans.filter(_.getParentSpanId.isEmpty)
      root.size should be(1)
      root.head.getSpanId should be("c")
    }

    it("should create an autogenerated span using the span tree with earliest timestamp if multiple trees exist with a root having an empty parentSpanId") {
      Given("trace with multiple roots ")
      val spanForest = MutableSpanForest(Seq(
        Span.newBuilder()
          .setSpanId("a")
          .setServiceName("aService")
          .setParentSpanId("")
          .addTags(Tag.newBuilder().setKey(SpanUtils.URL_TAG_KEY).setVStr("/anotherurl").setType(Tag.TagType.STRING))
          .setStartTime(150000000000l + 300)
          .build(),
        Span.newBuilder()
          .setSpanId("b")
          .setParentSpanId("")
          .setServiceName("bService")
          .addTags(Tag.newBuilder().setKey(SpanUtils.URL_TAG_KEY).setVStr("/someurl").setType(Tag.TagType.STRING))
          .setStartTime(150000000000l)
          .build(),
        Span.newBuilder()
          .setSpanId("c")
          .setServiceName("cService")
          .addTags(Tag.newBuilder().setKey(SpanUtils.URL_TAG_KEY).setVStr("/anotherurl").setType(Tag.TagType.STRING))
          .setParentSpanId("")
          .setStartTime(150000000000l + 150)
          .build()
      ))

      When("invoking transform")
      val transformedSpans = new InvalidRootTransformer().transform(spanForest).getUnderlyingSpans

      Then("pick earliest span tree as basis for autogenerated span")
      transformedSpans.length should be(4)

      val root = transformedSpans.filter(_.getParentSpanId.isEmpty)
      root.size should be(1)
      root.head.getSpanId should not be oneOf("a", "b", "c")
      root.head.getStartTime shouldBe 150000000000l
      root.head.getOperationName shouldEqual "auto-generated"
      root.head.getServiceName shouldEqual "bService"
      val urlTag = root.head.getTagsList.asScala.find(_.getKey == SpanUtils.URL_TAG_KEY)
      urlTag.isEmpty shouldBe false
      urlTag.get.getVStr shouldEqual "/someurl"
    }
  }
}
