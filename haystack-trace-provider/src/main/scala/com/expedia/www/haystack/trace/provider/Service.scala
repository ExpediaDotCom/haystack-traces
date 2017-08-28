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
package com.expedia.www.haystack.trace.provider

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.trace.provider.metrics.MetricsSupport
import com.expedia.www.haystack.trace.provider.providers.{FieldProvider, TraceProvider}
import com.expedia.www.haystack.trace.provider.config.ProviderConfiguration._
import com.expedia.www.haystack.trace.provider.stores.CassandraEsTraceStore
import io.grpc.netty.NettyServerBuilder
import io.grpc.Server
import org.slf4j.{Logger, LoggerFactory}

object Service extends MetricsSupport {
  private val LOGGER: Logger = LoggerFactory.getLogger("TraceProvider")
  private var jmxReporter: JmxReporter = _

  // primary executor for service's async tasks
  implicit private val executor = scala.concurrent.ExecutionContext.Implicits.global

  def main(args: Array[String]): Unit = {
    try {
      startJmxReporter()
      startService()
    }
    catch {
      case ex: Exception =>
        LOGGER.error("service failed with exception", ex)
        throw ex
    }
  }

  private def startJmxReporter() = {
    jmxReporter = JmxReporter.forRegistry(metricRegistry).build()
    jmxReporter.start()
  }

  private def startService(): Unit = {
    val store = new CassandraEsTraceStore(cassandraConfig, elasticSearchConfig)
    val server: Server = NettyServerBuilder
      .forPort(serviceConfig.port)
      .addService(new TraceProvider(store))
      .addService(new FieldProvider(store))
      .build
      .start

    LOGGER.info("server started, listening on 8080")
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        LOGGER.info("shutting down gRPC server since JVM is shutting down")
        server.shutdown()
        store.close()
        LOGGER.info("server shut down")
      }
    })

    server.awaitTermination()
  }
}
