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

package com.expedia.www.haystack.trace.reader.services

import com.expedia.www.haystack.trace.reader.metrics.MetricsSupport
import com.google.protobuf.GeneratedMessageV3
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
  * Handler for Grpc response
  * populates responseObserver with response object or error accordingly
  * takes care of corresponding logging and updating counters
  * @param operationName: name of operation
  * @param executor: executor service on which handler is invoked
  */

class GrpcHandler(operationName: String)(implicit val executor: ExecutionContextExecutor) extends MetricsSupport {
  private val logger = LoggerFactory.getLogger(s"${classOf[GrpcHandler]}.$operationName")

  private val timer = metricRegistry.timer(operationName)
  private val failureMeter = metricRegistry.meter(s"$operationName.failures")

  def handle[Rs](request: GeneratedMessageV3, responseObserver: StreamObserver[Rs])(op: => Future[Rs]): Unit = {
    val time = timer.time()
    op onComplete {
      case Success(response) =>
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        time.stop()
        logger.info(s"service invocation for operation={} and request={} completed successfully", operationName, request)

      case Failure(ex) =>
        responseObserver.onError(Status.fromThrowable(ex).asRuntimeException())
        failureMeter.mark()
        time.stop()
        logger.error(s"service invocation for operation={} and request={} failed with error", operationName, request, ex)
    }
  }
}
