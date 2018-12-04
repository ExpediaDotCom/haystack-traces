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

import java.util.concurrent.Future

import com.codahale.metrics.Timer
import com.expedia.open.tracing.backend.WriteSpansResponse
import com.expedia.open.tracing.backend.WriteSpansResponse.ResultCode
import com.expedia.www.haystack.commons.retries.RetryOperation
import com.expedia.www.haystack.trace.indexer.writers.grpc.TraceWriteResultListener
import org.easymock.EasyMock.anyObject
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, Matchers}

class TraceWriteResultListenerSpec extends FunSpec with Matchers with EasyMockSugar {
  describe("Trace Write Listener") {

    it("should run successfully without reporting any warnings") {

      val futureResponse = mock[Future[WriteSpansResponse]]
      val writeSpanResponse = WriteSpansResponse.newBuilder().setCode(ResultCode.SUCCESS).setErrorMessage("").build()
      val timer = mock[Timer.Context]
      val retryOp = mock[RetryOperation.Callback]

      expecting {
        retryOp.onResult(anyObject).once()
        timer.close().once()
        futureResponse.get().andReturn(writeSpanResponse).once()
      }
      whenExecuting(futureResponse, timer, retryOp) {
        val listener = new TraceWriteResultListener(futureResponse, timer, retryOp)
        listener.run()
      }
    }

    it("should run successfully without throwing any error even if asyncResult has errored") {
      val futureResponse = mock[Future[WriteSpansResponse]]
      val timer = mock[Timer.Context]
      val retryOp = mock[RetryOperation.Callback]

      val thrownException = new RuntimeException
      expecting {
        retryOp.onError(thrownException, retry = true)
        timer.close().once()
        futureResponse.get().andThrow(thrownException)
      }
      whenExecuting(futureResponse, timer, retryOp) {
        val listener = new TraceWriteResultListener(futureResponse, timer, retryOp)
        listener.run()
      }
    }
  }

}
