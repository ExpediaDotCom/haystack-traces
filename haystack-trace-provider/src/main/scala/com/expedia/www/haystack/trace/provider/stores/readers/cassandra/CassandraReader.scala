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

package com.expedia.www.haystack.trace.provider.stores.readers.cassandra

import com.datastax.driver.core.{BoundStatement, PreparedStatement}
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.expedia.open.tracing.api.Trace
import com.expedia.www.haystack.trace.provider.config.entities.CassandraConfiguration
import com.expedia.www.haystack.trace.provider.metrics.MetricsSupport
import com.expedia.www.haystack.trace.provider.stores.readers.cassandra.Schema._
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.Try

class CassandraReader(config: CassandraConfiguration)(implicit val dispatcher: ExecutionContextExecutor) extends MetricsSupport with AutoCloseable {
  private val LOGGER = LoggerFactory.getLogger(classOf[CassandraReader])

  private val readTimer = metricRegistry.timer("cassandra.read.time")
  private val readFailures = metricRegistry.meter("cassandra.read.failures")

  private val sessionFactory = new CassandraSessionFactory(config)

  private lazy val selectTrace: PreparedStatement = {
    import QueryBuilder.bindMarker

    sessionFactory.session.prepare(
      QueryBuilder
        .select()
        .from(config.tableName)
        .where(QueryBuilder.eq(ID_COLUMN_NAME, bindMarker(ID_COLUMN_NAME))))
  }

  private def constructSelectStatement(id: String) = new BoundStatement(selectTrace).setString(ID_COLUMN_NAME, id)

  def readTrace(traceId: String): Future[Trace] = {
    val timer = readTimer.time()
    val promise = Promise[Trace]

    try {
      val asyncResult = sessionFactory.session.executeAsync(constructSelectStatement(traceId))
      asyncResult.addListener(new CassandraReadResultListener(asyncResult, timer, readFailures, promise), dispatcher)

      promise.future
    } catch {
      case ex: Exception =>
        readFailures.mark()
        timer.stop()
        LOGGER.error("Failed to read trace with exception", ex)
        Future.failed(ex)
    }
  }

  override def close(): Unit = Try(sessionFactory.close())
}
