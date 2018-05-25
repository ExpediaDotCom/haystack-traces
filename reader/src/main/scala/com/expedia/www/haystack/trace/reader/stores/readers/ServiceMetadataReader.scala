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

import com.datastax.driver.core.BoundStatement
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraSession
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraTableSchema._
import com.expedia.www.haystack.trace.reader.config.entities.ServiceMetadataReadConfiguration
import com.expedia.www.haystack.trace.reader.metrics.{AppMetricNames, MetricsSupport}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}

/**
  * reader that lookup all service names and all operations for a given service name
  * @param cassandra: cassandra session used to do the lookup in the 'services' table
  * @param config: reader configuration
  * @param dispatcher: executor to run the read operations async
  */
class ServiceMetadataReader(cassandra: CassandraSession, config: ServiceMetadataReadConfiguration)
                           (implicit val dispatcher: ExecutionContextExecutor)
  extends MetricsSupport with AutoCloseable {

  private val LOGGER = LoggerFactory.getLogger(classOf[ServiceMetadataReader])

  private val readTimer = metricRegistry.timer(AppMetricNames.CASSANDRA_READ_TIME)
  private val readFailures = metricRegistry.meter(AppMetricNames.CASSANDRA_READ_FAILURES)

  private lazy val selectServiceOperationsPreparedStmt =
    cassandra.newSelectServicePreparedStatement(config.keyspace.name, config.keyspace.table)
  private lazy val selectAllServicesPreparedStmt =
    cassandra.newSelectAllServicesPreparedStatement(config.keyspace.name, config.keyspace.table)

  /**
    * fetch all operations for a given service name
    * @param serviceName: name of a service
    * @return future returning the list of operations of a given service name
    */
  def fetchServiceOperations(serviceName: String): Future[Seq[String]] = {
    val timer = readTimer.time()
    val promise = Promise[Seq[String]]

    try {
      LOGGER.info("Fetching all service operations for serviceName '{}'", serviceName)
      val stmt = new BoundStatement(selectServiceOperationsPreparedStmt).setString(SERVICE_COLUMN_NAME, serviceName)
      val asyncResult = cassandra.executeAsync(stmt)
      asyncResult.addListener(new ServiceMetadataResultListener(asyncResult, timer, readFailures, promise, (rows) => {
        LOGGER.info("successfully received operations for serviceName {} with total size - {}", serviceName, rows.size())
        rows.asScala.map(row => row.getString(OPERATION_COLUMN_NAME))
      }), dispatcher)
      promise.future
    } catch {
      case ex: Exception =>
        readFailures.mark()
        timer.stop()
        LOGGER.error(s"Failed to read all service operations for the service $serviceName with exception", ex)
        Future.failed(ex)
    }
  }

  /**
    * fetch all service names
    * @return future returning the list of service names
    */
  def fetchAllServiceNames(): Future[Seq[String]] = {
    val timer = readTimer.time()
    val promise = Promise[Seq[String]]

    try {
      val asyncResult = cassandra.executeAsync(new BoundStatement(selectAllServicesPreparedStmt))
      asyncResult.addListener(new ServiceMetadataResultListener(asyncResult, timer, readFailures, promise, (rows) => {
        rows.asScala.map(row => row.getString(SERVICE_COLUMN_NAME))
      }), dispatcher)
      promise.future
    } catch {
      case ex: Exception =>
        readFailures.mark()
        timer.stop()
        LOGGER.error("Failed to read all service names with exception", ex)
        Future.failed(ex)
    }
  }

  override def close(): Unit = ()
}
