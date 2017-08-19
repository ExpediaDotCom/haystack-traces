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

import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.open.tracing.{Process, Span, Tag}
import com.expedia.www.haystack.stitch.span.collector.config.entities.{IndexConfiguration, IndexField}
import com.expedia.www.haystack.stitch.span.collector.writers.es.index.document.IndexDocumentGenerator
import org.scalatest.{FunSpec, Matchers}

class SpanIndexDocumentGeneratorSpec extends FunSpec with Matchers {

  val TRACE_ID = "trace_id"
  describe("Span to IndexDocument Generator") {
    it ("should extract serviceName, operationName, duration and create json document for indexing") {
      val generator = new IndexDocumentGenerator(IndexConfiguration(Nil))

      val span_1 = Span.newBuilder().setTraceId(TRACE_ID)
        .setProcess(Process.newBuilder().setServiceName("service1"))
        .setOperationName("op1")
        .setDuration(600L)
        .build()
      val span_2 = Span.newBuilder().setTraceId(TRACE_ID)
        .setProcess(Process.newBuilder().setServiceName("service1"))
        .setOperationName("op1")
        .setDuration(500L)
        .build()
      val span_3 = Span.newBuilder().setTraceId(TRACE_ID)
        .setProcess(Process.newBuilder().setServiceName("service2"))
        .setDuration(1000L)
        .setOperationName("op3").build()

      val stitchedSpan = StitchedSpan.newBuilder().addChildSpans(span_1).addChildSpans(span_2).addChildSpans(span_3).setTraceId(TRACE_ID).build()
      val doc = generator.createIndexDocument(stitchedSpan).get
      doc.id should startWith(TRACE_ID)
      doc.stitchedSpanIndexJson shouldBe "{\"duration\":0,\"spans\":[{\"service\":\"service1\",\"operation\":\"op1\",\"duration\":600,\"tags\":{}},{\"service\":\"service1\",\"operation\":\"op1\",\"duration\":500,\"tags\":{}},{\"service\":\"service2\",\"operation\":\"op3\",\"duration\":1000,\"tags\":{}}]}"
    }

    it ("should not create an index document if service name is absent") {
      val generator = new IndexDocumentGenerator(IndexConfiguration(Nil))

      val span_1 = Span.newBuilder().setTraceId(TRACE_ID)
        .setOperationName("op1")
        .build()
      val span_2 = Span.newBuilder().setTraceId(TRACE_ID)
        .setDuration(1000L)
        .setOperationName("op2").build()

      val stitchedSpan = StitchedSpan.newBuilder().addChildSpans(span_1).addChildSpans(span_2).setTraceId(TRACE_ID).build()
      val doc = generator.createIndexDocument(stitchedSpan)
      doc shouldBe None
    }

    it ("should extract tags along with serviceName, operationName and duration and create json document for indexing") {
      val indexableTags = List(
        IndexField(name = "role", `type` = "string", true),
        IndexField(name = "errorCode", `type` = "long", true))
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
        .addTags(tag_1)
        .build()
      val span_3 = Span.newBuilder().setTraceId("traceId")
        .setProcess(Process.newBuilder().setServiceName("service2"))
        .setDuration(1000L)
        .addTags(tag_2)
        .setOperationName("op3").build()

      val stitchedSpan = StitchedSpan.newBuilder().addChildSpans(span_1).addChildSpans(span_2).addChildSpans(span_3).setTraceId(TRACE_ID).build()
      val doc = generator.createIndexDocument(stitchedSpan).get
      doc.id should startWith(TRACE_ID)
      doc.stitchedSpanIndexJson shouldBe "{\"duration\":0,\"spans\":[{\"service\":\"service1\",\"operation\":\"op1\",\"duration\":100,\"tags\":{\"role\":\"haystack\"}},{\"service\":\"service1\",\"operation\":\"op2\",\"duration\":200,\"tags\":{\"role\":\"haystack\",\"errorCode\":3}},{\"service\":\"service2\",\"operation\":\"op3\",\"duration\":1000,\"tags\":{\"errorCode\":3}}]}"
    }

    it ("should respect enabled flag of tags create right json document for indexing") {
      val indexableTags = List(
        IndexField(name = "role", `type` = "string", false),
        IndexField(name = "errorCode", `type` = "long", true))
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

      val stitchedSpan = StitchedSpan.newBuilder().addChildSpans(span_1).addChildSpans(span_2).setTraceId(TRACE_ID).build()
      val doc = generator.createIndexDocument(stitchedSpan).get
      doc.id should startWith(TRACE_ID)
      doc.stitchedSpanIndexJson shouldBe "{\"duration\":0,\"spans\":[{\"service\":\"service1\",\"operation\":\"op1\",\"duration\":100,\"tags\":{}},{\"service\":\"service1\",\"operation\":\"op2\",\"duration\":200,\"tags\":{\"errorCode\":3}}]}"
    }

    it ("one more test to verify the tags are indexed") {
      val indexableTags = List(
        IndexField(name = "errorCode", `type` = "long", true))
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

      val stitchedSpan = StitchedSpan.newBuilder().addChildSpans(span_1).addChildSpans(span_2).setTraceId(TRACE_ID).build()
      val doc = generator.createIndexDocument(stitchedSpan).get
      doc.id should startWith(TRACE_ID)
      doc.stitchedSpanIndexJson shouldBe "{\"duration\":0,\"spans\":[{\"service\":\"service1\",\"operation\":\"op1\",\"duration\":100,\"tags\":{\"errorCode\":5}},{\"service\":\"service1\",\"operation\":\"op2\",\"duration\":200,\"tags\":{\"errorCode\":3}}]}"
    }

    it ("should extract unique tag values along with serviceName, operationName and duration and create json document for indexing") {
      val indexableTags = List(
        IndexField(name = "role", `type` = "string"),
        IndexField(name = "errorCode", `type` = "long"))
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

      val stitchedSpan = StitchedSpan.newBuilder().addChildSpans(span_1).addChildSpans(span_2).setTraceId(TRACE_ID).build()
      val doc = generator.createIndexDocument(stitchedSpan).get
      doc.id should startWith(TRACE_ID)
      doc.stitchedSpanIndexJson shouldBe "{\"duration\":0,\"spans\":[{\"service\":\"service1\",\"operation\":\"op1\",\"duration\":100,\"tags\":{\"role\":\"haystack\"}},{\"service\":\"service1\",\"operation\":\"op1\",\"duration\":200,\"tags\":{\"role\":\"haystack\"}}]}"
    }
  }
}
