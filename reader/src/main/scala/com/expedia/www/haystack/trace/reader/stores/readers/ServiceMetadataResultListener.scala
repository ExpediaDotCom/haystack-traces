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

package com.expedia.www.haystack.trace.reader.stores.readers

import com.codahale.metrics.{Meter, Timer}
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.{ResultSetFuture, Row}
import com.expedia.www.haystack.commons.health.HealthController
import com.expedia.www.haystack.trace.reader.stores.readers.ServiceMetadataResultListener._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Promise
import scala.util.{Failure, Success, Try}

object ServiceMetadataResultListener {
  protected val LOGGER: Logger = LoggerFactory.getLogger(classOf[ServiceMetadataResultListener])
}

class ServiceMetadataResultListener(asyncResult: ResultSetFuture,
                                    timer: Timer.Context,
                                    failure: Meter,
                                    promise: Promise[Seq[String]],
                                    transform: java.util.List[Row] => Seq[String]) extends Runnable {
  override def run(): Unit = {

    Try(asyncResult.get).map(rs => transform(rs.all())) match {
      case Success(trace) =>
        promise.success(trace)
      case Failure(ex) =>
        if (fatalError(ex)) {
          LOGGER.error("Fatal error in reading from cassandra, tearing down the app", ex)
          HealthController.setUnhealthy()
        } else {
          LOGGER.error("Failed in reading the record from cassandra", ex)
        }
        failure.mark()
        promise.failure(ex)
    }
  }

  private def fatalError(ex: Throwable): Boolean = {
    if (ex.isInstanceOf[NoHostAvailableException]) true else ex.getCause != null && fatalError(ex.getCause)
  }
}
