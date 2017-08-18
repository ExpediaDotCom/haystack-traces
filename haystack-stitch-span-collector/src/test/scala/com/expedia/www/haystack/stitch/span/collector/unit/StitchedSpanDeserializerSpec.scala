package com.expedia.www.haystack.stitch.span.collector.unit

import com.expedia.open.tracing.Span
import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.stitch.span.collector.serdes.StitchedSpanDeserializer
import org.scalatest.{FunSpec, Matchers}

class StitchedSpanDeserializerSpec extends FunSpec with Matchers {
  val traceId = "TRACEID_1"
  val spanId = "SPANID_1"
  val parentId = "PARENTID_1"

  describe("Stitched span deserializer") {
    it("should deserialize the stitched span bytes") {
      val deser = new StitchedSpanDeserializer()
      val span = Span.newBuilder().setTraceId(traceId).setDuration(100).setSpanId(spanId).setParentSpanId(parentId)
      val stitchedSpan = StitchedSpan.newBuilder().setTraceId(traceId).addChildSpans(span).build()
      val obj = deser.deserialize("topic", stitchedSpan.toByteArray)
      obj.getTraceId shouldEqual traceId
      obj.getChildSpansCount shouldBe 1
      obj.getChildSpans(0).getTraceId shouldEqual traceId
      obj.getChildSpans(0).getParentSpanId shouldEqual parentId
      obj.getChildSpans(0).getSpanId shouldEqual spanId
      obj.getChildSpans(0).getDuration shouldBe 100
    }

    it("should return null if deserialize the empty data bytes") {
      val deser = new StitchedSpanDeserializer()
      deser.deserialize("", Array.emptyByteArray) shouldBe null
    }

    it("should return null if deserialize the illegal data bytes") {
      val span = Span.newBuilder().setTraceId(traceId).setDuration(100).setSpanId(spanId).setParentSpanId(parentId).build()
      val deser = new StitchedSpanDeserializer()
      deser.deserialize("", span.toByteArray) shouldBe null
    }
  }
}
