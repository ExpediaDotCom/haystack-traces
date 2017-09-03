package com.expedia.www.haystack.trace.indexer.unit

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trace.indexer.store.DynamicCacheSizer
import com.expedia.www.haystack.trace.indexer.store.impl.SpanBufferMemoryStore
import org.apache.kafka.streams.processor.{ProcessorContext, StateRestoreCallback, StateStore}
import org.easymock.EasyMock
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, Matchers}
import scala.collection.JavaConversions._

class SpanBufferMemoryStoreSpec extends FunSpec with Matchers with EasyMockSugar {

  private val TRACE_ID_1 = "TraceId_1"
  private val TRACE_ID_2 = "TraceId_2"

  describe("SpanBuffer Memory Store") {
    it("should create spanBuffer, add child spans and allow retrieving old spanBuffers from the store") {
      val (context, rootStateStore, spanBufferStore) = createSpanBufferStore
      whenExecuting(context, rootStateStore) {
        spanBufferStore.init(context, rootStateStore)
        val span1 = Span.newBuilder().setTraceId(TRACE_ID_1).setSpanId("SPAN_ID_1").build()
        val span2 = Span.newBuilder().setTraceId(TRACE_ID_1).setSpanId("SPAN_ID_2").build()

        spanBufferStore.addOrUpdateSpanBuffer(TRACE_ID_1, span1, 11000L)
        spanBufferStore.addOrUpdateSpanBuffer(TRACE_ID_1, span2, 12000L)

        spanBufferStore.totalSpans shouldBe 2

        val result = spanBufferStore.getAndRemoveSpanBuffersOlderThan(13000L)
        result.size() shouldBe 1
        result.foreach {
          case (traceId, spanBufferWithMetadata) =>
            traceId shouldBe TRACE_ID_1
            spanBufferWithMetadata.builder.getChildSpansCount shouldBe 2
            spanBufferWithMetadata.builder.getChildSpans(0).getSpanId shouldBe "SPAN_ID_1"
            spanBufferWithMetadata.builder.getChildSpans(1).getSpanId shouldBe "SPAN_ID_2"
        }
        spanBufferStore.totalSpans shouldBe 0
      }
    }

    it("should create two spanBuffers for different traceIds, allow retrieving old spanBuffers from the store") {
      val (context, rootStateStore, spanBufferStore) = createSpanBufferStore
      whenExecuting(context, rootStateStore) {
        spanBufferStore.init(context, rootStateStore)
        val span1 = Span.newBuilder().setTraceId(TRACE_ID_1).setSpanId("SPAN_ID_1").build()
        val span2 = Span.newBuilder().setTraceId(TRACE_ID_2).setSpanId("SPAN_ID_2").build()

        spanBufferStore.addOrUpdateSpanBuffer(TRACE_ID_1, span1, 11000L)
        spanBufferStore.addOrUpdateSpanBuffer(TRACE_ID_2, span2, 12000L)

        spanBufferStore.totalSpans shouldBe 2

        var result = spanBufferStore.getAndRemoveSpanBuffersOlderThan(11500L)
        result.size() shouldBe 1
        result.foreach {
          case (traceId, spanBufferWithMetadata) =>
            traceId shouldBe TRACE_ID_1
            spanBufferWithMetadata.builder.getChildSpansCount shouldBe 1
            spanBufferWithMetadata.builder.getChildSpans(0).getSpanId shouldBe "SPAN_ID_1"
        }

        spanBufferStore.totalSpans shouldBe 1

        result = spanBufferStore.getAndRemoveSpanBuffersOlderThan(12500L)
        result.size() shouldBe 1
        result.foreach {
          case (traceId, spanBufferWithMetadata) =>
            traceId shouldBe TRACE_ID_2
            spanBufferWithMetadata.builder.getChildSpansCount shouldBe 1
            spanBufferWithMetadata.builder.getChildSpans(0).getSpanId shouldBe "SPAN_ID_2"
        }

        spanBufferStore.totalSpans shouldBe 0
      }
    }
  }

  private def createSpanBufferStore = {
    val cacheSizer = new DynamicCacheSizer(10, 1000)
    val spanBufferStore = new SpanBufferMemoryStore("spanBuffer", cacheSizer)
    val context = mock[ProcessorContext]
    val rootStateStore = mock[StateStore]

    expecting {
      context.applicationId().andReturn("appId").anyTimes()
      context.register(EasyMock.anyObject(classOf[StateStore]), EasyMock.anyBoolean(), EasyMock.anyObject(classOf[StateRestoreCallback])).anyTimes()
    }
    (context, rootStateStore, spanBufferStore)
  }
}
