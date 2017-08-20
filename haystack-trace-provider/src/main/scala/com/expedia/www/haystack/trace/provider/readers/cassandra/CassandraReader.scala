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

package com.expedia.www.haystack.trace.provider.readers.cassandra

import com.datastax.driver.core.ResultSet
import com.expedia.open.tracing.internal.Trace
import com.expedia.www.haystack.trace.provider.config.entities.CassandraConfiguration
import com.expedia.www.haystack.trace.provider.metrics.MetricsSupport
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.Try

class CassandraReader(config: CassandraConfiguration)(implicit val dispatcher: ExecutionContextExecutor) extends MetricsSupport with AutoCloseable {

  private val LOGGER = LoggerFactory.getLogger(classOf[CassandraReader])

  private val sessionFactory = new CassandraSessionFactory(config)

  def read(query: String): Future[Trace] = {
    val promise = Promise[Trace]
    val asyncResult = sessionFactory.session.executeAsync(query)

    asyncResult.addListener(new Runnable {
      override def run(): Unit = promise.success(createTrace(asyncResult.get()))
    }, dispatcher)

    promise.future
  }

  private def createTrace(set: ResultSet): Trace = {
    // TODO parse result set and generate Trace
    null
  }

  override def close(): Unit = Try(sessionFactory.close())
}
