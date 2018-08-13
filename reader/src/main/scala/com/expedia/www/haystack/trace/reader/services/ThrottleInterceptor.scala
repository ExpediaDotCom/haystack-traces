/*
 *  Copyright 2018 Expedia, Inc.
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

package com.expedia.www.haystack.trace.reader.services

import java.util.concurrent.Semaphore

import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor}

class ThrottleInterceptor(concurrencyByMethodNames: Map[String, Int]) extends ServerInterceptor {
  private val semaphoresByMethodNames = concurrencyByMethodNames.map {
    case (name, limit) => name -> new Semaphore(limit)
  }

  override def interceptCall[ReqT, RespT](call: ServerCall[ReqT, RespT],
                                          metadata: Metadata,
                                          next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    semaphoresByMethodNames.get(call.getMethodDescriptor.getFullMethodName) match {
      case Some(semaphore) =>
        try {
          semaphore.acquire()
          next.startCall(call, metadata)
        } finally {
          semaphore.release()
        }
      case _ =>
        next.startCall(call, metadata)
    }
  }
}