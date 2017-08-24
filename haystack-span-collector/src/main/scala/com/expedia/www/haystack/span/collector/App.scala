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

package com.expedia.www.haystack.span.collector

import akka.actor.ActorSystem
import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.span.collector.config.ProjectConfiguration
import com.expedia.www.haystack.span.collector.metrics.MetricsSupport
import com.expedia.www.haystack.span.collector.writers.cassandra.CassandraWriter
import com.expedia.www.haystack.span.collector.writers.es.ElasticSearchWriter
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object App extends MetricsSupport {
  private val LOGGER = LoggerFactory.getLogger(App.getClass)

  def main(args: Array[String]): Unit = {
    try {
      startApp()
    } catch {
      case ex: Exception =>
        LOGGER.error("Fail to run the application with exception:", ex)
        System.exit(1)
    }
  }

  private def startApp(): Unit = {
    startJmxReporter()

    val project = new ProjectConfiguration

    implicit val system = ActorSystem.create("span-collector-system", project.config)

    val cassandraWriter = new CassandraWriter(project.cassandraConfig)(system.dispatcher)
    val elasticSearchWriter = new ElasticSearchWriter(project.elasticSearchConfig, project.indexConfig)

    val topology = new StreamTopology(project.collectorConfig, List(cassandraWriter, elasticSearchWriter))
    val (killSwitch, streamResult) = topology.start()

    // add shutdown hook to kill the stream topology
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = killSwitch.shutdown()
    })

    // attach onComplete event of the stream topology and tear down the actor system and all writers
    streamResult.onComplete {
      case Success(_) =>
        LOGGER.info("Span collector stream has completed with success!!")
        shutdown()
      case Failure(reason) =>
        LOGGER.error("Span collector has closed with following error, tearing down the app", reason)
        shutdown()
    } (system.dispatcher)

    // shutdown all the clients(elastic search and cassandra) and akka's actor system
    def shutdown(): Unit = {
      elasticSearchWriter.close()
      cassandraWriter.close()
      project.close()
      LOGGER.info("Terminating the actor system..")
      system.terminate()
    }

    Await.ready(system.whenTerminated, Duration.Inf)
  }

  private def startJmxReporter(): Unit = {
    val jmxReporter = JmxReporter.forRegistry(metricRegistry).build()
    jmxReporter.start()
  }
}
