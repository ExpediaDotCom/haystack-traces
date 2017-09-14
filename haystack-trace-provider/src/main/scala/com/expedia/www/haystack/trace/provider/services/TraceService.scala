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

package com.expedia.www.haystack.trace.provider.services

import com.expedia.open.tracing.Span
import com.expedia.open.tracing.api._
import com.expedia.www.haystack.trace.provider.providers.TraceProvider
import com.expedia.www.haystack.trace.provider.stores.TraceStore
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContextExecutor

class TraceService(traceStore: TraceStore)(implicit val executor: ExecutionContextExecutor) extends TraceProviderGrpc.TraceProviderImplBase {
  private val handleGetTraceResponse = new GrpcHandler(TraceProviderGrpc.METHOD_GET_TRACE.getFullMethodName)
  private val handleGetRawTraceResponse = new GrpcHandler(TraceProviderGrpc.METHOD_GET_RAW_TRACE.getFullMethodName)
  private val handleGetRawSpanResponse = new GrpcHandler(TraceProviderGrpc.METHOD_GET_RAW_SPAN.getFullMethodName)
  private val handleSearchResponse = new GrpcHandler(TraceProviderGrpc.METHOD_SEARCH_TRACES.getFullMethodName)

  private val traceProvider: TraceProvider = new TraceProvider(traceStore)

  /**
    * endpoint for fetching a trace
    * trace will be validated and transformed
    * @param request TraceRequest object containing traceId of the trace to fetch
    * @param responseObserver response observer will contain Trace object
    *                         or will error out with [[com.expedia.www.haystack.trace.provider.exceptions.TraceNotFoundException]]
    */
  override def getTrace(request: TraceRequest, responseObserver: StreamObserver[Trace]): Unit = {
    handleGetTraceResponse.handle(responseObserver) {
      traceProvider.getTrace(request)
    }
  }

  /**
    * endpoint for fetching raw trace logs, trace will returned without validations and transformations
    * @param request TraceRequest object containing traceId of the trace to fetch
    * @param responseObserver response observer will stream out [[Trace]] object
    *                         or will error out with [[com.expedia.www.haystack.trace.provider.exceptions.TraceNotFoundException]]
    */
  override def getRawTrace(request: TraceRequest, responseObserver: StreamObserver[Trace]): Unit = {
    handleGetRawTraceResponse.handle(responseObserver) {
      traceProvider.getRawTrace(request)
    }
  }

  /**
    * endpoint for fetching raw span logs, span will returned without validations and transformations
    * @param request SpanRequest object containing spanId and parent traceId of the span to fetch
    * @param responseObserver response observer will stream out [[Span]] object
    *                         or will error out with [[com.expedia.www.haystack.trace.provider.exceptions.SpanNotFoundException]]
    */
  override def getRawSpan(request: SpanRequest, responseObserver: StreamObserver[Span]): Unit = {
    handleGetRawSpanResponse.handle(responseObserver) {
      traceProvider.getRawSpan(request)
    }
  }

  /**
    * endpoint for searching traces
    * @param request TracesSearchRequest object containing criteria and filters for traces to find
    * @param responseObserver response observer will stream out [[List[Trace]]
    */
  override def searchTraces(request: TracesSearchRequest, responseObserver: StreamObserver[TracesSearchResult]): Unit = {
    handleSearchResponse.handle(responseObserver) {
      traceProvider.searchTraces(request)
    }
  }
}
