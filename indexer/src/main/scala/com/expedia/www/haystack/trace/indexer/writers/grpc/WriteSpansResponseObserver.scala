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

package com.expedia.www.haystack.trace.indexer.writers.grpc

import com.codahale.metrics.Timer
import com.expedia.open.tracing.backend.WriteSpansResponse
import com.expedia.www.haystack.commons.retries.RetryOperation
import io.grpc.stub.StreamObserver



class WriteSpansResponseObserver(timer: Timer.Context,
                                 retryOp: RetryOperation.Callback) extends StreamObserver[WriteSpansResponse] {

  /**
    * this is invoked when the grpc aysnc write completes.
    * We measure the time write operation takes and records any warnings or errors
    */

  override def onNext(writeSpanResponse: WriteSpansResponse): Unit = {
    retryOp.onResult(writeSpanResponse)
  }

  override def onError(error: Throwable): Unit = {
    retryOp.onError(error, retry = true)
  }

  override def onCompleted(): Unit = {
  }
}