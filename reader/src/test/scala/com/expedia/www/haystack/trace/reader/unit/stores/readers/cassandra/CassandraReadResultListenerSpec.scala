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
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.open.tracing.api.{Field, Trace, TraceRequest, TracesSearchRequest}
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraTableSchema
import com.expedia.www.haystack.trace.reader.stores.readers.cassandra.CassandraReadResultListener
import com.expedia.www.haystack.trace.reader.stores.readers.es.query.TraceSearchQueryGenerator
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec
import io.grpc.{Status, StatusException}
import org.easymock.EasyMock

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.concurrent.Promise

class CassandraReadResultListenerSpec extends BaseUnitTestSpec {

  describe("cassandra write listener") {
    it("") {
      val p = new TraceSearchQueryGenerator("haystack-traces", "spans", "spans")
      val field = Field.newBuilder().setName("serviceName").setValue("expweb").build()
      val s = p.generate(TracesSearchRequest.newBuilder().setStartTime(1510469157572000l).setEndTime(1510469161172000l).setLimit(40).addFields(field).build())
      println(s.toString)
    }
    it("should read the rows, deserialized spans column and return the complete trace") {
      val mockReadResult = mock[ResultSetFuture]
      val resultSet = mock[ResultSet]
      val promise = mock[Promise[Trace]]
      val failureMeter = mock[Meter]
      val timer = mock[Timer.Context]

      val mockSpanBufferRow_1 = mock[Row]
      val mockSpanBufferRow_2 = mock[Row]

      val span_1 = Span.newBuilder().setTraceId("TRACE_ID").setSpanId("SPAN_ID_1")
      val span_2 = Span.newBuilder().setTraceId("TRACE_ID").setSpanId("SPAN_ID_2")
      val spanBuffer_1 = SpanBuffer.newBuilder().setTraceId("TRACE_ID").addChildSpans(span_1).build()
      val spanBuffer_2 = SpanBuffer.newBuilder().setTraceId("TRACE_ID").addChildSpans(span_2).build()

      val capturedTrace = EasyMock.newCapture[Trace]()
      expecting {
        timer.close()
        mockReadResult.get().andReturn(resultSet)
        resultSet.all().andReturn(List(mockSpanBufferRow_1, mockSpanBufferRow_2).asJava)
        mockSpanBufferRow_1.getBytes(CassandraTableSchema.SPANS_COLUMN_NAME).andReturn(ByteBuffer.wrap(spanBuffer_1.toByteArray))
        mockSpanBufferRow_2.getBytes(CassandraTableSchema.SPANS_COLUMN_NAME).andReturn(ByteBuffer.wrap(spanBuffer_2.toByteArray))
        promise.success(EasyMock.capture(capturedTrace)).andReturn(promise)
      }

      whenExecuting(mockReadResult, promise, failureMeter, timer,resultSet,  mockSpanBufferRow_1, mockSpanBufferRow_2) {
        val listener = new CassandraReadResultListener(mockReadResult, timer, failureMeter, promise)
        listener.run()
        capturedTrace.getValue.getChildSpansList.map(_.getSpanId) should contain allOf("SPAN_ID_1", "SPAN_ID_2")
        capturedTrace.getValue.getTraceId shouldBe "TRACE_ID"
      }
    }

    it("should read the rows, fail to deserialized spans column and then return an empty trace") {
      val mockReadResult = mock[ResultSetFuture]
      val resultSet = mock[ResultSet]
      val promise = mock[Promise[Trace]]
      val failureMeter = mock[Meter]
      val timer = mock[Timer.Context]

      val mockSpanBufferRow_1 = mock[Row]
      val mockSpanBufferRow_2 = mock[Row]

      val span_1 = Span.newBuilder().setTraceId("TRACE_ID").setSpanId("SPAN_ID_1")
      val spanBuffer_1 = SpanBuffer.newBuilder().setTraceId("TRACE_ID").addChildSpans(span_1).build()

      expecting {
        timer.close()
        failureMeter.mark()
        mockReadResult.get().andReturn(resultSet)
        resultSet.all().andReturn(List(mockSpanBufferRow_1, mockSpanBufferRow_2).asJava)
        mockSpanBufferRow_1.getBytes(CassandraTableSchema.SPANS_COLUMN_NAME).andReturn(ByteBuffer.wrap(spanBuffer_1.toByteArray))
        mockSpanBufferRow_2.getBytes(CassandraTableSchema.SPANS_COLUMN_NAME).andReturn(ByteBuffer.wrap("illegal bytes".getBytes))
        promise.failure(EasyMock.anyObject()).andReturn(promise)
      }

      whenExecuting(mockReadResult, promise, failureMeter, timer,resultSet,  mockSpanBufferRow_1, mockSpanBufferRow_2) {
        val listener = new CassandraReadResultListener(mockReadResult, timer, failureMeter, promise)
        listener.run()
      }
    }

    it("should return an exception for empty traceId") {
      val mockReadResult = mock[ResultSetFuture]
      val resultSet = mock[ResultSet]
      val promise = mock[Promise[Trace]]
      val failureMeter = mock[Meter]
      val timer = mock[Timer.Context]

      val capturedException = EasyMock.newCapture[StatusException]()
      expecting {
        timer.close()
        failureMeter.mark()
        mockReadResult.get().andReturn(resultSet)
        resultSet.all().andReturn(List[Row]().asJava)
        promise.failure(EasyMock.capture(capturedException)).andReturn(promise)
      }

      whenExecuting(mockReadResult, promise, failureMeter, timer,resultSet) {
        val listener = new CassandraReadResultListener(mockReadResult, timer, failureMeter, promise)
        listener.run()
        capturedException.getValue.getStatus.getCode shouldEqual Status.NOT_FOUND.getCode
      }
    }
  }
}
