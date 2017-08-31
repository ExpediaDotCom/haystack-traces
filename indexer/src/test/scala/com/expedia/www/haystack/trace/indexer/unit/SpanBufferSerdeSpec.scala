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
import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.trace.indexer.serde.SpanBufferSerde
import org.scalatest.{FunSpec, Matchers}

class SpanBufferSerdeSpec extends FunSpec with Matchers {

  private val TRACE_ID = "unique-trace-id"
  private val PARENT_SPAN_ID = "parent-span-id"
  private val SPAN_ID = "spanId-1"
  private val OP_NAME = "testOp"
  private val TAG_KEY = "tag-key"
  private val TAG_VALUE = "tag-value"
  private val TOPIC = "topic"

  private val newSpanBuffer = {
    val tag = Tag.newBuilder().setType(Tag.TagType.STRING).setKey(TAG_KEY).setVStr(TAG_VALUE).build()
    val span = Span.newBuilder()
      .setTraceId(TRACE_ID)
      .setParentSpanId(PARENT_SPAN_ID)
      .setSpanId(SPAN_ID)
      .setOperationName(OP_NAME)
      .addTags(tag)
      .build()
    SpanBuffer.newBuilder().setTraceId(TRACE_ID).addChildSpans(span).build()
  }

  describe("SpanBuffer Serde") {
    it("should serialize and deserialize a buffered span object") {
      val ser = new SpanBufferSerde().serializer.serialize(TOPIC, newSpanBuffer)
      val deser = new SpanBufferSerde().deserializer.deserialize(TOPIC, ser)
      deser.getTraceId shouldEqual TRACE_ID
      deser.getChildSpansCount shouldBe 1

      val span = deser.getChildSpans(0)
      span.getParentSpanId shouldEqual PARENT_SPAN_ID
      span.getTraceId shouldEqual TRACE_ID
      span.getSpanId shouldEqual SPAN_ID
      span.getOperationName shouldEqual OP_NAME
      span.getTagsCount shouldBe 1

      val tag = span.getTags(0)
      tag.getType shouldBe Tag.TagType.STRING
      tag.getKey shouldBe TAG_KEY
      tag.getVStr shouldBe TAG_VALUE
    }

    it("should return null on serializing invalid span buffer bytes") {
      val data = "invalid buffered span serialized bytes".getBytes()
      val deser = new SpanBufferSerde().deserializer.deserialize("topic", data)
      deser shouldBe null
    }
  }
}
