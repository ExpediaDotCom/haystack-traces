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

import com.codahale.metrics.{Meter, Timer}
import com.expedia.www.haystack.trace.provider.metrics.MetricsSupport
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
  * Handler for Grpc response
  * populates responseObserver with response object or error accordingly
  * takes care of corresponding logging and updating counters
  * @param operationName
  * @param executor
  */
class GrpcHandler(operationName: String)(implicit val executor: ExecutionContextExecutor) extends MetricsSupport {
  val logger: Logger = LoggerFactory.getLogger(s"${classOf[GrpcHandler]}.$operationName")

  val timer: Timer = metricRegistry.timer(operationName)
  val failures: Meter = metricRegistry.meter(s"${operationName}.failures")

  def handle[Rs](responseObserver: StreamObserver[Rs])
                (op: => Future[Rs]) = {
    val time = timer.time()
    op onComplete {
      case Success(response) =>
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        time.stop()
        logger.info(s"service invocation completed successfully")

      case Failure(th) =>
        responseObserver.onError(Status.fromThrowable(th).asRuntimeException())
        failures.mark()
        time.stop()
        logger.error(s"service invocation failed", th)
    }
  }
}
