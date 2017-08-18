/*
 *  Copyright 2017 Expedia, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.expedia.www.haystack.stitch.span.collector.unit

import com.expedia.open.tracing.{Process, Span, Tag}
import com.expedia.www.haystack.stitch.span.collector.config.entities.{IndexAttribute, IndexConfiguration}
import com.expedia.www.haystack.stitch.span.collector.writers.es.index.generator.IndexDocumentGenerator
import org.scalatest.{FunSpec, Matchers}

class SpanIndexDocumentGeneratorSpec extends FunSpec with Matchers {

  describe("Span to IndexDocument Generator") {
    it ("should extract serviceName, operationName, duration and create json document for indexing") {
      val generator = new IndexDocumentGenerator(IndexConfiguration(Nil))

      val span_1 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service1"))
        .setOperationName("op1")
        .setDuration(600L)
        .build()
      val span_2 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service1"))
        .setOperationName("op1")
        .setDuration(500L)
        .build()
      val span_3 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service2"))
        .setDuration(1000L)
        .setOperationName("op3").build()

      val doc = generator.create("traceId", List(span_1, span_2, span_3)).get
      doc.id should startWith("traceId_")
      doc.indexJson shouldBe "{\"service2\":{\"_all\":{\"tags\":{},\"minduration\":1000,\"maxduration\":1000},\"op3\":{\"tags\":{},\"minduration\":1000,\"maxduration\":1000}},\"service1\":{\"_all\":{\"tags\":{},\"minduration\":500,\"maxduration\":600},\"op1\":{\"tags\":{},\"minduration\":500,\"maxduration\":600}}}"
    }

    it ("should not create an index document if service name is absent") {
      val generator = new IndexDocumentGenerator(IndexConfiguration(Nil))

      val span_1 = Span.newBuilder().setTraceId("traceId")
        .setOperationName("op1")
        .build()
      val span_2 = Span.newBuilder().setTraceId("traceId")
        .setDuration(1000L)
        .setOperationName("op2").build()

      val doc = generator.create("traceId", List(span_1, span_2))
      doc shouldBe None
    }

    it ("should extract tags along with serviceName, operationName and duration and create json document for indexing") {
      val indexableTags = List(
        IndexAttribute(name = "role", `type` = "string", true),
        IndexAttribute(name = "errorCode", `type` = "long", true))
      val generator = new IndexDocumentGenerator(IndexConfiguration(indexableTags))

      val tag_1 = Tag.newBuilder().setKey("role").setType(Tag.TagType.STRING).setVStr("haystack").build()
      val tag_2  = Tag.newBuilder().setKey("errorCode").setType(Tag.TagType.LONG).setVLong(3).build()

      val span_1 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service1"))
        .setOperationName("op1")
        .setDuration(100L)
        .addTags(tag_1)
        .build()
      val span_2 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service1"))
        .setOperationName("op2")
        .setDuration(200L)
        .addTags(tag_2)
        .build()
      val span_3 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service2"))
        .setDuration(1000L)
        .addTags(tag_2)
        .setOperationName("op3").build()

      val doc = generator.create("traceId", List(span_1, span_2, span_3)).get
      doc.id should startWith("traceId_")
      doc.indexJson shouldBe "{\"service2\":{\"_all\":{\"tags\":{\"errorCode\":[3]},\"minduration\":1000,\"maxduration\":1000},\"op3\":{\"tags\":{\"errorCode\":[3]},\"minduration\":1000,\"maxduration\":1000}},\"service1\":{\"_all\":{\"tags\":{\"errorCode\":[3],\"role\":[\"haystack\"]},\"minduration\":100,\"maxduration\":200},\"op2\":{\"tags\":{\"errorCode\":[3]},\"minduration\":200,\"maxduration\":200},\"op1\":{\"tags\":{\"role\":[\"haystack\"]},\"minduration\":100,\"maxduration\":100}}}"
    }

    it ("should respect enabled flag of tags create right json document for indexing") {
      val indexableTags = List(
        IndexAttribute(name = "role", `type` = "string", false),
        IndexAttribute(name = "errorCode", `type` = "long", true))
      val generator = new IndexDocumentGenerator(IndexConfiguration(indexableTags))

      val tag_1 = Tag.newBuilder().setKey("role").setType(Tag.TagType.STRING).setVStr("haystack").build()
      val tag_2  = Tag.newBuilder().setKey("errorCode").setType(Tag.TagType.LONG).setVLong(3).build()

      val span_1 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service1"))
        .setOperationName("op1")
        .setDuration(100L)
        .addTags(tag_1)
        .build()
      val span_2 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service1"))
        .setOperationName("op2")
        .setDuration(200L)
        .addTags(tag_2)
        .build()

      val doc = generator.create("traceId", List(span_1, span_2)).get
      doc.id should startWith("traceId_")
      doc.indexJson shouldBe "{\"service1\":{\"_all\":{\"tags\":{\"errorCode\":[3]},\"minduration\":100,\"maxduration\":200},\"op2\":{\"tags\":{\"errorCode\":[3]},\"minduration\":200,\"maxduration\":200},\"op1\":{\"tags\":{},\"minduration\":100,\"maxduration\":100}}}"
    }

    it ("should merge tag values with the same key and create right json document for indexing") {
      val indexableTags = List(
        IndexAttribute(name = "errorCode", `type` = "long", true))
      val generator = new IndexDocumentGenerator(IndexConfiguration(indexableTags))

      val tag_1 = Tag.newBuilder().setKey("errorCode").setType(Tag.TagType.LONG).setVLong(5).build()
      val tag_2  = Tag.newBuilder().setKey("errorCode").setType(Tag.TagType.LONG).setVLong(3).build()

      val span_1 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service1"))
        .setOperationName("op1")
        .setDuration(100L)
        .addTags(tag_1)
        .build()
      val span_2 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service1"))
        .setOperationName("op2")
        .setDuration(200L)
        .addTags(tag_2)
        .build()

      val doc = generator.create("traceId", List(span_1, span_2)).get
      doc.id should startWith("traceId_")
      doc.indexJson shouldBe "{\"service1\":{\"_all\":{\"tags\":{\"errorCode\":[5,3]},\"minduration\":100,\"maxduration\":200},\"op2\":{\"tags\":{\"errorCode\":[3]},\"minduration\":200,\"maxduration\":200},\"op1\":{\"tags\":{\"errorCode\":[5]},\"minduration\":100,\"maxduration\":100}}}"
    }

    it ("should extract unique tag values along with serviceName, operationName and duration and create json document for indexing") {
      val indexableTags = List(
        IndexAttribute(name = "role", `type` = "string"),
        IndexAttribute(name = "errorCode", `type` = "long"))
      val generator = new IndexDocumentGenerator(IndexConfiguration(indexableTags))

      val tag_1 = Tag.newBuilder().setKey("role").setType(Tag.TagType.STRING).setVStr("haystack").build()
      val tag_2  = Tag.newBuilder().setKey("error").setType(Tag.TagType.LONG).setVLong(3).build()

      val span_1 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service1"))
        .setOperationName("op1")
        .setDuration(100L)
        .addTags(tag_1)
        .build()
      val span_2 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service1"))
        .setOperationName("op1")
        .setDuration(200L)
        .addTags(tag_1)
        .build()
      val span_3 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service2"))
        .setDuration(1000L)
        .addTags(tag_2)
        .setOperationName("op3").build()

      val doc = generator.create("traceId", List(span_1, span_2, span_3)).get
      doc.id should startWith("traceId_")
      doc.indexJson shouldBe "{\"service2\":{\"_all\":{\"tags\":{},\"minduration\":1000,\"maxduration\":1000},\"op3\":{\"tags\":{},\"minduration\":1000,\"maxduration\":1000}},\"service1\":{\"_all\":{\"tags\":{\"role\":[\"haystack\"]},\"minduration\":100,\"maxduration\":200},\"op1\":{\"tags\":{\"role\":[\"haystack\"]},\"minduration\":100,\"maxduration\":200}}}"
    }
  }
}
