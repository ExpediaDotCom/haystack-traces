/*
 *  Copyright 2019 Expedia, Inc.
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
package com.expedia.www.haystack.trace.storage.backends.mysql

import java.io.File

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.commons.logger.LoggerUtils
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trace.storage.backends.mysql.client.{MysqlTableSchema, SqlConnectionManager}
import com.expedia.www.haystack.trace.storage.backends.mysql.config.ProjectConfiguration
import com.expedia.www.haystack.trace.storage.backends.mysql.services.{GrpcHealthService, SpansPersistenceService}
import com.expedia.www.haystack.trace.storage.backends.mysql.store.{MysqlTraceRecordReader, MysqlTraceRecordWriter}
import io.grpc.netty.NettyServerBuilder
import org.slf4j.{Logger, LoggerFactory}

object Service extends MetricsSupport {
  private val LOGGER: Logger = LoggerFactory.getLogger("MysqlBackend")

  // primary executor for service's async tasks
  implicit private val executor = scala.concurrent.ExecutionContext.global

  def main(args: Array[String]): Unit = {
    startJmxReporter()
    startService()
  }

  private def startJmxReporter(): Unit = {
    JmxReporter
      .forRegistry(metricRegistry)
      .build()
      .start()
  }

  private def startService(): Unit = {
    try {
      val config = new ProjectConfiguration
      val serviceConfig = config.serviceConfig
      val sqlConnectionManager = new SqlConnectionManager(config.mysqlConfig.clientConfig)

      MysqlTableSchema.ensureExists(config.mysqlConfig.databaseConfig.autoCreateSchema, sqlConnectionManager.getConnection)

      val tracerRecordWriter = new MysqlTraceRecordWriter(config.mysqlConfig,sqlConnectionManager)
      val tracerRecordReader = new MysqlTraceRecordReader(config.mysqlConfig.clientConfig,sqlConnectionManager)

      val serverBuilder = NettyServerBuilder
        .forPort(serviceConfig.port)
        .directExecutor()
        .addService(new GrpcHealthService())
        .addService(new SpansPersistenceService(reader = tracerRecordReader, writer = tracerRecordWriter)(executor))

      // enable ssl if enabled
      if (serviceConfig.ssl.enabled) {
        serverBuilder.useTransportSecurity(new File(serviceConfig.ssl.certChainFilePath), new File(serviceConfig.ssl.privateKeyPath))
      }

      val server = serverBuilder.build().start()

      LOGGER.info(s"server started, listening on ${serviceConfig.port}")

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run(): Unit = {
          LOGGER.info("shutting down gRPC server since JVM is shutting down")
          server.shutdown()
          LOGGER.info("server has been shutdown now")
        }
      })

      server.awaitTermination()
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        LOGGER.error("Fatal error observed while running the app", ex)
        LoggerUtils.shutdownLogger()
        System.exit(1)
    }
  }
}
