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

package com.expedia.www.haystack.trace.indexer

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.trace.indexer.config.ProjectConfiguration
import com.expedia.www.haystack.trace.indexer.metrics.MetricsSupport
import org.slf4j.LoggerFactory

object App extends MetricsSupport {
  private val LOGGER = LoggerFactory.getLogger(App.getClass)

  def main(args: Array[String]): Unit = {
    startJmxReporter()

    try {
      val appConfig = new ProjectConfiguration

      val stream = new StreamRunner(
        appConfig.kafkaConfig,
        appConfig.spanAccumulateConfig,
        appConfig.elasticSearchConfig,
        appConfig.cassandraConfig,
        appConfig.indexConfig)

      Runtime.getRuntime.addShutdownHook(new Thread {
        override def run(): Unit = {
          LOGGER.info("Shutdown hook is invoked, tearing down the application.")
          stream.close()
          appConfig.close()
        }
      })

      stream.start()
    } catch {
      case ex: Exception =>
        LOGGER.error("Observed fatal exception while running the app", ex)
        System.exit(1)
    }
  }

  private def startJmxReporter() = {
    val jmxReporter = JmxReporter.forRegistry(metricRegistry).build()
    jmxReporter.start()
  }
}
