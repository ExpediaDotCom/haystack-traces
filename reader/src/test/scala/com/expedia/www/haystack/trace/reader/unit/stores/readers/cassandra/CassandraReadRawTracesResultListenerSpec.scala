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

package com.expedia.www.haystack.trace.reader.unit.stores.readers.cassandra

import java.nio.ByteBuffer

import com.codahale.metrics.{Meter, Timer}
import com.datastax.driver.core.{ResultSet, ResultSetFuture, Row}
import com.expedia.open.tracing.Span
import com.expedia.open.tracing.api.Trace
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraTableSchema
import com.expedia.www.haystack.trace.reader.stores.readers.cassandra.CassandraReadRawTracesResultListener
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec
import io.grpc.{Status, StatusException}
import org.easymock.EasyMock

import scala.collection.JavaConverters._
import scala.concurrent.Promise

class CassandraReadRawTracesResultListenerSpec extends BaseUnitTestSpec {

  describe("cassandra read listener for raw traces") {
    it("should read the rows, de-serialized spans column and return the complete trace") {
      val mockReadResult = mock[ResultSetFuture]
      val resultSet = mock[ResultSet]
      val promise = mock[Promise[Seq[Trace]]]
      val failureMeter = mock[Meter]
      val tracesFailures = mock[Meter]
      val timer = mock[Timer.Context]

      val mockSpanBufferRow_1 = mock[Row]
      val mockSpanBufferRow_2 = mock[Row]
      val mockSpanBufferRow_3 = mock[Row]

      val span_1 = Span.newBuilder().setTraceId("TRACE_ID1").setSpanId("SPAN_ID_1")
      val span_2 = Span.newBuilder().setTraceId("TRACE_ID1").setSpanId("SPAN_ID_2")
      val spanBuffer_1 = SpanBuffer.newBuilder().setTraceId("TRACE_ID1").addChildSpans(span_1).build()
      val spanBuffer_2 = SpanBuffer.newBuilder().setTraceId("TRACE_ID1").addChildSpans(span_2).build()

      val span_3 = Span.newBuilder().setTraceId("TRACE_ID3").setSpanId("SPAN_ID_3")
      val spanBuffer_3 = SpanBuffer.newBuilder().setTraceId("TRACE_ID3").addChildSpans(span_3).build()


      val capturedTraces = EasyMock.newCapture[Seq[Trace]]()
      val capturedMeter = EasyMock.newCapture[Int]()
      expecting {
        timer.close()
        tracesFailures.mark(EasyMock.capture(capturedMeter))
        mockReadResult.get().andReturn(resultSet)
        resultSet.all().andReturn(List(mockSpanBufferRow_1, mockSpanBufferRow_2, mockSpanBufferRow_3).asJava)
        mockSpanBufferRow_1.getBytes(CassandraTableSchema.SPANS_COLUMN_NAME).andReturn(ByteBuffer.wrap(spanBuffer_1.toByteArray))
        mockSpanBufferRow_2.getBytes(CassandraTableSchema.SPANS_COLUMN_NAME).andReturn(ByteBuffer.wrap(spanBuffer_2.toByteArray))
        mockSpanBufferRow_3.getBytes(CassandraTableSchema.SPANS_COLUMN_NAME).andReturn(ByteBuffer.wrap(spanBuffer_3.toByteArray))
        promise.success(EasyMock.capture(capturedTraces)).andReturn(promise)
      }

      whenExecuting(mockReadResult, promise, tracesFailures, failureMeter, timer, resultSet, mockSpanBufferRow_1, mockSpanBufferRow_2, mockSpanBufferRow_3) {
        val listener = new CassandraReadRawTracesResultListener(mockReadResult, promise, timer, failureMeter, tracesFailures, 2)
        listener.run()
        val traceIdSpansMap: Map[String, Set[String]] = capturedTraces.getValue.map(capturedTrace =>
          capturedTrace.getTraceId -> capturedTrace.getChildSpansList.asScala.map(_.getSpanId).toSet).toMap

        traceIdSpansMap("TRACE_ID1") shouldEqual Set("SPAN_ID_1", "SPAN_ID_2")
        traceIdSpansMap("TRACE_ID3") shouldEqual Set("SPAN_ID_3")

        capturedMeter.getValue shouldEqual 0
      }
    }

    it("should read the rows, fail to de-serialized spans column and then return an empty trace") {
      val mockReadResult = mock[ResultSetFuture]
      val resultSet = mock[ResultSet]
      val promise = mock[Promise[Seq[Trace]]]
      val failureMeter = mock[Meter]
      val tracesFailures = mock[Meter]
      val timer = mock[Timer.Context]

      val mockSpanBufferRow_1 = mock[Row]
      val mockSpanBufferRow_2 = mock[Row]

      val span_1 = Span.newBuilder().setTraceId("TRACE_ID").setSpanId("SPAN_ID_1")
      val spanBuffer_1 = SpanBuffer.newBuilder().setTraceId("TRACE_ID").addChildSpans(span_1).build()

      val capturedMeter = EasyMock.newCapture[Int]()
      expecting {
        timer.close()
        failureMeter.mark()
        tracesFailures.mark(EasyMock.capture(capturedMeter))
        mockReadResult.get().andReturn(resultSet)
        resultSet.all().andReturn(List(mockSpanBufferRow_1, mockSpanBufferRow_2).asJava)
        mockSpanBufferRow_1.getBytes(CassandraTableSchema.SPANS_COLUMN_NAME).andReturn(ByteBuffer.wrap(spanBuffer_1.toByteArray))
        mockSpanBufferRow_2.getBytes(CassandraTableSchema.SPANS_COLUMN_NAME).andReturn(ByteBuffer.wrap("illegal bytes".getBytes))
        promise.failure(EasyMock.anyObject()).andReturn(promise)
      }

      whenExecuting(mockReadResult, promise, failureMeter, tracesFailures, timer, resultSet, mockSpanBufferRow_1, mockSpanBufferRow_2) {
        val listener = new CassandraReadRawTracesResultListener(mockReadResult, promise, timer, failureMeter, tracesFailures, 1)
        listener.run()
        capturedMeter.getValue shouldEqual 1
      }
    }

    it("should return an exception for empty traceId") {
      val mockReadResult = mock[ResultSetFuture]
      val resultSet = mock[ResultSet]
      val promise = mock[Promise[Seq[Trace]]]
      val failureMeter = mock[Meter]
      val tracesFailures = mock[Meter]
      val timer = mock[Timer.Context]

      val capturedException = EasyMock.newCapture[StatusException]()
      val capturedMeter = EasyMock.newCapture[Int]()
      expecting {
        timer.close()
        failureMeter.mark()
        tracesFailures.mark(EasyMock.capture(capturedMeter))
        mockReadResult.get().andReturn(resultSet)
        resultSet.all().andReturn(List[Row]().asJava)
        promise.failure(EasyMock.capture(capturedException)).andReturn(promise)
      }

      whenExecuting(mockReadResult, promise, failureMeter, tracesFailures, timer, resultSet) {
        val listener = new CassandraReadRawTracesResultListener(mockReadResult, promise, timer, failureMeter, tracesFailures, 0)
        listener.run()
        capturedException.getValue.getStatus.getCode shouldEqual Status.NOT_FOUND.getCode
        capturedMeter.getValue shouldEqual 0
      }
    }
  }
}
