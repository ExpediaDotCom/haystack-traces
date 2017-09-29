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

package com.expedia.www.haystack.trace.indexer.unit

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.open.tracing.{Log, Span, Tag}
import com.expedia.www.haystack.trace.commons.config.entities.{WhiteListIndexFields, WhitelistIndexField, WhitelistIndexFieldConfiguration}
import com.expedia.www.haystack.trace.indexer.writers.es.IndexDocumentGenerator
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.scalatest.{FunSpec, Matchers}

class SpanIndexDocumentGeneratorSpec extends FunSpec with Matchers {
  implicit val formats = DefaultFormats

  val TRACE_ID = "trace_id"
  describe("Span to IndexDocument Generator") {
    it ("should extract serviceName, operationName, duration and create json document for indexing") {
      val generator = new IndexDocumentGenerator(WhitelistIndexFieldConfiguration())

      val span_1 = Span.newBuilder().setTraceId(TRACE_ID)
        .setSpanId("span-1")
        .setServiceName("service1")
        .setOperationName("op1")
        .setDuration(600L)
        .build()
      val span_2 = Span.newBuilder().setTraceId(TRACE_ID)
        .setSpanId("span-2")
        .setServiceName("service1")
        .setOperationName("op1")
        .setDuration(500L)
        .build()
      val span_3 = Span.newBuilder().setTraceId(TRACE_ID)
        .setSpanId("span-3")
        .setServiceName("service2")
        .setDuration(1000L)
        .setOperationName("op3").build()

      val spanBuffer = SpanBuffer.newBuilder().addChildSpans(span_1).addChildSpans(span_2).addChildSpans(span_3).setTraceId(TRACE_ID).build()
      val doc = generator.createIndexDocument(TRACE_ID, spanBuffer).get
      doc.id should startWith(TRACE_ID)
      doc.json shouldBe "{\"rootDuration\":0,\"spans\":[{\"operation\":\"op1\",\"spanid\":\"span-1\",\"service\":\"service1\",\"duration\":600},{\"operation\":\"op1\",\"spanid\":\"span-2\",\"service\":\"service1\",\"duration\":500},{\"operation\":\"op3\",\"spanid\":\"span-3\",\"service\":\"service2\",\"duration\":1000}]}"
    }

    it ("should not create an index document if service name is absent") {
      val generator = new IndexDocumentGenerator(WhitelistIndexFieldConfiguration())

      val span_1 = Span.newBuilder().setTraceId(TRACE_ID)
        .setOperationName("op1")
        .build()
      val span_2 = Span.newBuilder().setTraceId(TRACE_ID)
        .setDuration(1000L)
        .setOperationName("op2").build()

      val spanBuffer = SpanBuffer.newBuilder().addChildSpans(span_1).addChildSpans(span_2).setTraceId(TRACE_ID).build()
      val doc = generator.createIndexDocument(TRACE_ID, spanBuffer)
      doc shouldBe None
    }

    it ("should extract tags along with serviceName, operationName and duration and create json document for indexing") {
      val indexableTags = List(
        WhitelistIndexField(name = "role", `type` = "string"),
        WhitelistIndexField(name = "errorCode", `type` = "long"))

      val whitelistConfig = WhitelistIndexFieldConfiguration()
      whitelistConfig.onReload(Serialization.write(WhiteListIndexFields(indexableTags)))
      val generator = new IndexDocumentGenerator(whitelistConfig)

      val tag_1 = Tag.newBuilder().setKey("role").setType(Tag.TagType.STRING).setVStr("haystack").build()
      val tag_2  = Tag.newBuilder().setKey("errorCode").setType(Tag.TagType.LONG).setVLong(3).build()

      val span_1 = Span.newBuilder().setTraceId("traceId")
        .setServiceName("service1")
        .setSpanId("span-1")
        .setOperationName("op1")
        .setDuration(100L)
        .addTags(tag_1)
        .build()
      val span_2 = Span.newBuilder().setTraceId("traceId")
        .setServiceName("service1")
        .setSpanId("span-2")
        .setOperationName("op2")
        .setDuration(200L)
        .addTags(tag_2)
        .addTags(tag_1)
        .build()
      val span_3 = Span.newBuilder().setTraceId("traceId")
        .setSpanId("span-3")
        .setServiceName("service2")
        .setDuration(1000L)
        .addTags(tag_2)
        .setOperationName("op3").build()

      val spanBuffer = SpanBuffer.newBuilder().addChildSpans(span_1).addChildSpans(span_2).addChildSpans(span_3).setTraceId(TRACE_ID).build()
      val doc = generator.createIndexDocument(TRACE_ID, spanBuffer).get
      doc.id should startWith(TRACE_ID)
      doc.json shouldBe "{\"rootDuration\":0,\"spans\":[{\"operation\":\"op1\",\"role\":[\"haystack\"],\"spanid\":\"span-1\",\"service\":\"service1\",\"duration\":100},{\"operation\":\"op2\",\"role\":[\"haystack\"],\"errorCode\":[3],\"spanid\":\"span-2\",\"service\":\"service1\",\"duration\":200},{\"operation\":\"op3\",\"errorCode\":[3],\"spanid\":\"span-3\",\"service\":\"service2\",\"duration\":1000}]}"
    }

    it ("should respect enabled flag of tags create right json document for indexing") {
      val indexableTags = List(
        WhitelistIndexField(name = "role", `type` = "string", enabled = false),
        WhitelistIndexField(name = "errorCode", `type` = "long"))
      val whitelistConfig = WhitelistIndexFieldConfiguration()
      whitelistConfig.onReload(Serialization.write(WhiteListIndexFields(indexableTags)))
      val generator = new IndexDocumentGenerator(whitelistConfig)

      val tag_1 = Tag.newBuilder().setKey("role").setType(Tag.TagType.STRING).setVStr("haystack").build()
      val tag_2  = Tag.newBuilder().setKey("errorCode").setType(Tag.TagType.LONG).setVLong(3).build()

      val span_1 = Span.newBuilder().setTraceId("traceId")
        .setSpanId("span-1")
        .setServiceName("service1")
        .setOperationName("op1")
        .setDuration(100L)
        .addTags(tag_1)
        .build()
      val span_2 = Span.newBuilder().setTraceId("traceId")
        .setSpanId("span-2")
        .setServiceName("service1")
        .setOperationName("op2")
        .setDuration(200L)
        .addTags(tag_2)
        .build()

      val spanBuffer = SpanBuffer.newBuilder().addChildSpans(span_1).addChildSpans(span_2).setTraceId(TRACE_ID).build()
      val doc = generator.createIndexDocument(TRACE_ID, spanBuffer).get
      doc.id should startWith(TRACE_ID)
      doc.json shouldBe "{\"rootDuration\":0,\"spans\":[{\"operation\":\"op1\",\"spanid\":\"span-1\",\"service\":\"service1\",\"duration\":100},{\"operation\":\"op2\",\"errorCode\":[3],\"spanid\":\"span-2\",\"service\":\"service1\",\"duration\":200}]}"
    }

    it ("one more test to verify the tags are indexed") {
      val indexableTags = List(
        WhitelistIndexField(name = "errorCode", `type` = "long"))
      val whitelistConfig = WhitelistIndexFieldConfiguration()
      whitelistConfig.onReload(Serialization.write(WhiteListIndexFields(indexableTags)))
      val generator = new IndexDocumentGenerator(whitelistConfig)

      val tag_1 = Tag.newBuilder().setKey("errorCode").setType(Tag.TagType.LONG).setVLong(5).build()
      val tag_2  = Tag.newBuilder().setKey("errorCode").setType(Tag.TagType.LONG).setVLong(3).build()

      val span_1 = Span.newBuilder().setTraceId("traceId")
        .setServiceName("service1")
        .setSpanId("span-1")
        .setOperationName("op1")
        .setDuration(100L)
        .addTags(tag_1)
        .build()
      val span_2 = Span.newBuilder().setTraceId("traceId")
        .setServiceName("service1")
        .setSpanId("span-2")
        .setOperationName("op2")
        .setDuration(200L)
        .addTags(tag_2)
        .build()

      val spanBuffer = SpanBuffer.newBuilder().addChildSpans(span_1).addChildSpans(span_2).setTraceId(TRACE_ID).build()
      val doc = generator.createIndexDocument(TRACE_ID, spanBuffer).get
      doc.id should startWith(TRACE_ID)
      doc.json shouldBe "{\"rootDuration\":0,\"spans\":[{\"operation\":\"op1\",\"errorCode\":[5],\"spanid\":\"span-1\",\"service\":\"service1\",\"duration\":100},{\"operation\":\"op2\",\"errorCode\":[3],\"spanid\":\"span-2\",\"service\":\"service1\",\"duration\":200}]}"
    }

    it ("should extract unique tag values along with serviceName, operationName and duration and create json document for indexing") {
      val indexableTags = List(
        WhitelistIndexField(name = "role", `type` = "string"),
        WhitelistIndexField(name = "errorCode", `type` = "long"))
      val whitelistConfig = WhitelistIndexFieldConfiguration()
      whitelistConfig.onReload(Serialization.write(WhiteListIndexFields(indexableTags)))
      val generator = new IndexDocumentGenerator(whitelistConfig)

      val tag_1 = Tag.newBuilder().setKey("role").setType(Tag.TagType.STRING).setVStr("haystack").build()
      val tag_2  = Tag.newBuilder().setKey("errorCode").setType(Tag.TagType.LONG).setVLong(3).build()

      val span_1 = Span.newBuilder().setTraceId("traceId")
        .setServiceName("service1")
        .setSpanId("span-1")
        .setOperationName("op1")
        .setDuration(100L)
        .addTags(tag_1)
        .build()
      val span_2 = Span.newBuilder().setTraceId("traceId")
        .setServiceName("service1")
        .setSpanId("span-2")
        .setOperationName("op2")
        .setDuration(200L)
        .addTags(tag_2)
        .build()

      val spanBuffer = SpanBuffer.newBuilder().addChildSpans(span_1).addChildSpans(span_2).setTraceId(TRACE_ID).build()
      val doc = generator.createIndexDocument(TRACE_ID, spanBuffer).get
      doc.id should startWith(TRACE_ID)
      doc.json shouldBe "{\"rootDuration\":0,\"spans\":[{\"operation\":\"op1\",\"role\":[\"haystack\"],\"spanid\":\"span-1\",\"service\":\"service1\",\"duration\":100},{\"operation\":\"op2\",\"errorCode\":[3],\"spanid\":\"span-2\",\"service\":\"service1\",\"duration\":200}]}"
    }

    it ("should extract tags, log values along with serviceName, operationName and duration and create json document for indexing") {
      val indexableTags = List(
        WhitelistIndexField(name = "role", `type` = "string"),
        WhitelistIndexField(name = "errorCode", `type` = "long"),
        WhitelistIndexField(name = "exception", `type` = "string"))

      val whitelistConfig = WhitelistIndexFieldConfiguration()
      whitelistConfig.onReload(Serialization.write(WhiteListIndexFields(indexableTags)))
      val generator = new IndexDocumentGenerator(whitelistConfig)

      val tag_1 = Tag.newBuilder().setKey("role").setType(Tag.TagType.STRING).setVStr("haystack").build()
      val tag_2  = Tag.newBuilder().setKey("errorCode").setType(Tag.TagType.LONG).setVLong(3).build()
      val log_1 = Log.newBuilder()
        .addFields(Tag.newBuilder().setKey("exception").setType(Tag.TagType.STRING).setVStr("xxx-yy-zzz").build())
        .setTimestamp(100L)
      val log_2 = Log.newBuilder()
        .addFields(Tag.newBuilder().setKey("exception").setType(Tag.TagType.STRING).setVStr("aaa-bb-cccc").build())
        .setTimestamp(200L)

      val span_1 = Span.newBuilder().setTraceId("traceId")
        .setServiceName("service1")
        .setSpanId("span-1")
        .setOperationName("op1")
        .setDuration(100L)
        .addTags(tag_1)
        .build()
      val span_2 = Span.newBuilder().setTraceId("traceId")
        .setServiceName("service1")
        .setSpanId("span-2")
        .setOperationName("op2")
        .setDuration(200L)
        .addTags(tag_2)
        .addLogs(log_1)
        .addLogs(log_2)
        .build()

      val spanBuffer = SpanBuffer.newBuilder().addChildSpans(span_1).addChildSpans(span_2).setTraceId(TRACE_ID).build()
      val doc = generator.createIndexDocument(TRACE_ID, spanBuffer).get
      doc.id should startWith(TRACE_ID)
      doc.json shouldBe "{\"rootDuration\":0,\"spans\":[{\"operation\":\"op1\",\"role\":[\"haystack\"],\"spanid\":\"span-1\",\"service\":\"service1\",\"duration\":100},{\"operation\":\"op2\",\"errorCode\":[3],\"spanid\":\"span-2\",\"service\":\"service1\",\"duration\":200,\"exception\":[\"xxx-yy-zzz\",\"aaa-bb-cccc\"]}]}"
    }
  }
}
