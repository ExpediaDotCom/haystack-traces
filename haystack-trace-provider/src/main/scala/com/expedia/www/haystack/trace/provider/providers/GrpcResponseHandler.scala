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

import com.codahale.metrics.{Meter, Timer}
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.Logger

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

trait GrpcResponseHandler {

  def handle[Rs](logger: Logger, timer: Timer, failures: Meter)
                (responseObserver: StreamObserver[Rs])
                (op: => Future[Rs]) = {
    val time = timer.time()

    val responseFuture = op

    responseFuture onComplete {
      case Success(response) =>
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        time.stop()
        logger.info("")

      case Failure(th) =>
        responseObserver.onError(Status.fromThrowable(th).asRuntimeException())
        failures.mark()
        time.stop()
        logger.error("", th)
    }
  }
}
