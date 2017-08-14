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

package com.expedia.www.haystack.stitch.span.collector

import akka.actor.ActorSystem
import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.stitch.span.collector.config.ProjectConfiguration
import com.expedia.www.haystack.stitch.span.collector.config.ProjectConfiguration._
import com.expedia.www.haystack.stitch.span.collector.metrics.MetricsSupport
import com.expedia.www.haystack.stitch.span.collector.writers.cassandra.{CassandraSessionFactory, CassandraWriter}
import com.expedia.www.haystack.stitch.span.collector.writers.es.ElasticSearchWriter
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Collector extends MetricsSupport {
  private val LOGGER = LoggerFactory.getLogger(Collector.getClass)

  def main(args: Array[String]): Unit = {
    startJmxReporter()

    implicit val system = ActorSystem.create("stitched-span-collector-system", ProjectConfiguration.config)
    implicit val dispatcher = system.dispatcher

    val cassandraWriter = new CassandraWriter(cassandraConfig)
    val elasticSearchWriter = new ElasticSearchWriter(elasticSearchConfig)

    val topology = new StreamTopology(collectorConfig, List(cassandraWriter, elasticSearchWriter))
    val (killSwitch, streamResult) = topology.start()

    // add shutdown hook to kill the stream topology
    Runtime.getRuntime.addShutdownHook(new Thread() {
      killSwitch.shutdown()
    })

    // attach onComplete event of the stream topology and tear down the actor system and all writers
    streamResult.onComplete {
      case Success(_) =>
        LOGGER.info("Stream has completed with success!!")
        shutdown()
      case Failure(reason) =>
        LOGGER.error("Stream has been closed with error, shutting down the actor system", reason)
        shutdown()
    }

    def shutdown(): Unit = {
      system.terminate()
      elasticSearchWriter.close()
      cassandraWriter.close()
    }

    Await.ready(system.whenTerminated, Duration.Inf)
  }

  private def startJmxReporter(): Unit = {
    val jmxReporter = JmxReporter.forRegistry(metricRegistry).build()
    jmxReporter.start()
  }
}
