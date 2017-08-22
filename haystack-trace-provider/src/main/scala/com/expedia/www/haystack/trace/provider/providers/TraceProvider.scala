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

package com.expedia.www.haystack.trace.provider.providers

import com.expedia.open.tracing.Span
import com.expedia.open.tracing.internal._
import com.expedia.www.haystack.trace.provider.exceptions.SpanNotFoundException
import com.expedia.www.haystack.trace.provider.stores.TraceStore
import io.grpc.stub.StreamObserver

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global

class TraceProvider(traceStore: TraceStore) extends TraceProviderGrpc.TraceProviderImplBase {
  val handleGetTraceResponse = new GrpcResponseHandler[Trace](TraceProviderGrpc.METHOD_GET_TRACE.getFullMethodName)
  val handleGetRawTraceResponse = new GrpcResponseHandler[Trace](TraceProviderGrpc.METHOD_GET_RAW_TRACE.getFullMethodName)
  val handleGetRawSpanResponse = new GrpcResponseHandler[Span](TraceProviderGrpc.METHOD_GET_RAW_SPAN.getFullMethodName)
  val handleSearchResponse = new GrpcResponseHandler[TracesSearchResult](TraceProviderGrpc.METHOD_SEARCH_TRACES.getFullMethodName)

  def transform(trace: Trace): Trace = trace

  override def getTrace(request: TraceRequest, responseObserver: StreamObserver[Trace]): Unit = {
    handleGetTraceResponse.handle(responseObserver) {
      traceStore
        .getTrace(request.getTraceId)
        .map(transform)
    }
  }

  override def getRawTrace(request: TraceRequest, responseObserver: StreamObserver[Trace]): Unit = {
    handleGetRawTraceResponse.handle(responseObserver) {
      traceStore.getTrace(request.getTraceId)
    }
  }

  override def getRawSpan(request: SpanRequest, responseObserver: StreamObserver[Span]): Unit = {
    handleGetRawSpanResponse.handle(responseObserver) {
      traceStore.getTrace(request.getTraceId).map(trace => {
        val spanOption = trace.getChildSpansList
          .find(span => span.getSpanId.equalsIgnoreCase(request.getSpanId))

        spanOption match {
          case Some(span) => span
          case None => throw new SpanNotFoundException
        }
      })
    }
  }

  override def searchTraces(request: TracesSearchRequest, responseObserver: StreamObserver[TracesSearchResult]): Unit = {
    handleSearchResponse.handle(responseObserver) {
      traceStore.searchTraces(request)
    }
  }
}
