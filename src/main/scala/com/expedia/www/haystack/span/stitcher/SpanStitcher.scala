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
package com.expedia.www.haystack.span.stitcher

import java.util.concurrent.TimeUnit

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.span.stitcher.config.ProjectConfiguration
import com.expedia.www.haystack.span.stitcher.metrics.MetricsSupport
import org.apache.kafka.streams.KafkaStreams

object SpanStitcher extends MetricsSupport {
  import ProjectConfiguration._

  private var jmxReporter: JmxReporter = _
  private var kstreams: KafkaStreams = _

  def main(args: Array[String]): Unit = {
    startJmxReporter()

    kstreams = new StreamTopology(kafkaConfig, spansConfig).start()
    Runtime.getRuntime.addShutdownHook(new ShutdownHookThread)
  }

  private def startJmxReporter() = {
    jmxReporter = JmxReporter.forRegistry(metricRegistry).build()
    jmxReporter.start()
  }

  private class ShutdownHookThread extends Thread {
    override def run(): Unit = {
      if(kstreams != null) kstreams.close(30, TimeUnit.SECONDS)
      if(jmxReporter != null) jmxReporter.close()
    }
  }
}
