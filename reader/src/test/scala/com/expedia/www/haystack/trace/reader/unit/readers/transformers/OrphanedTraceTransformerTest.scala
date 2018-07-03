package com.expedia.www.haystack.trace.reader.unit.readers.transformers

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trace.reader.readers.transformers.{OrphanedTraceTransformer, OrphanedTraceTransformerConstants}
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec

class OrphanedTraceTransformerTest extends BaseUnitTestSpec {
  describe("OrphanedTraceTransformerTest") {
    it("should return full list of spans if there is a root span already") {
      val span_1 = Span.newBuilder().setTraceId("traceId").setSpanId("traceId").setServiceName("another-service").build()
      val span_2 = Span.newBuilder().setTraceId("traceId").setSpanId("span_2").setParentSpanId(span_1.getSpanId).setServiceName("test-service").build()
      val span_3 = Span.newBuilder().setTraceId("traceId").setSpanId("span_3").setParentSpanId(span_1.getSpanId).setServiceName("another-service").build()

      val transformer = new OrphanedTraceTransformer()
      val spans = transformer.transform(List(span_1, span_2, span_3))
      spans.size shouldBe 3
      spans should contain(span_1)
      spans should contain(span_2)
      spans should contain(span_3)
    }

    it("should return the full list of spans plus a generated root span if there is no root span already") {
      val span_1 = Span.newBuilder().setTraceId("traceId").setOperationName(OrphanedTraceTransformerConstants.UNKNOWN_OPERATION_NAME).setServiceName(OrphanedTraceTransformerConstants.UNKNOWN_SERVICE_NAME).setSpanId("traceId").setStartTime(10000).setDuration(10100).build()
      val span_2 = Span.newBuilder().setTraceId("traceId").setSpanId("span_2").setParentSpanId(span_1.getSpanId).setStartTime(10000).setDuration(10).setServiceName("test-service").build()
      val span_3 = Span.newBuilder().setTraceId("traceId").setSpanId("span_3").setParentSpanId(span_1.getSpanId).setStartTime(20000).setDuration(100).setServiceName("another-service").build()

      val transformer = new OrphanedTraceTransformer()
      val spans = transformer.transform(List(span_2, span_3))
      spans.size shouldBe 3
      spans should contain(span_2)
      spans should contain(span_3)
      spans should contain(span_1)
    }

    it("should fail if there are multiple different orphaned parent ids") {
      val span_1 = Span.newBuilder().setTraceId("traceId").setSpanId("traceId").setServiceName("another-service").build()
      val span_2 = Span.newBuilder().setTraceId("traceId").setSpanId("span_2").setParentSpanId(span_1.getSpanId).setServiceName("test-service").build()
      val span_3 = Span.newBuilder().setTraceId("traceId").setSpanId("span_3").setParentSpanId(span_1.getSpanId).setServiceName("another-service").build()
      val span_4 = Span.newBuilder().setTraceId("traceId").setSpanId("span_4").setParentSpanId(span_1.getSpanId).setServiceName("another-service").build()
      val span_5 = Span.newBuilder().setTraceId("traceId").setSpanId("span_5").setParentSpanId(span_4.getSpanId).setServiceName("another-service").build()

      val transformer = new OrphanedTraceTransformer()
      val spans = transformer.transform(List(span_2, span_3, span_5))
      spans.size shouldBe 0
    }

    it("should fail if there is a missing span in between the root span and orphaned span") {
      val span_1 = Span.newBuilder().setTraceId("traceId").setSpanId("traceId").setServiceName("another-service").build()
      val span_4 = Span.newBuilder().setTraceId("traceId").setSpanId("span_4").setParentSpanId(span_1.getSpanId).setServiceName("another-service").build()
      val span_5 = Span.newBuilder().setTraceId("traceId").setSpanId("span_5").setParentSpanId(span_4.getSpanId).setServiceName("another-service").build()

      val transformer = new OrphanedTraceTransformer()
      val spans = transformer.transform(List(span_5))
      spans.size shouldBe 0
    }
  }
}
