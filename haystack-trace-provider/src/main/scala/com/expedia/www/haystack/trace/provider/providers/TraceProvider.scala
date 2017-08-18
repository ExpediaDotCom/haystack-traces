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

import com.expedia.open.tracing.internal._
import io.grpc.Status
import io.grpc.stub.StreamObserver

class TraceProvider extends TraceProviderGrpc.TraceProviderImplBase {
  override def searchTraces(request: TracesSearchRequest, responseObserver: StreamObserver[TracesSearchResult]): Unit = {
    try {
      val searchResult = TracesSearchResult.newBuilder().addTraces(new Trace()).build()

      responseObserver.onNext(searchResult)
      responseObserver.onCompleted()
    } catch {
      case th:Throwable => responseObserver.onError(Status.fromThrowable(th).asRuntimeException())
    }
  }

  override def getTrace(request: TraceRequest, responseObserver: StreamObserver[Trace]): Unit = super.getTrace(request, responseObserver)
}
