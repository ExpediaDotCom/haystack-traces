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

import com.expedia.www.haystack.span.stitcher.config.entities.{KafkaConfiguration, SpanConfiguration}
import com.expedia.www.haystack.span.stitcher.processors.SpanStitchProcessSupplier
import com.expedia.www.haystack.span.stitcher.serde.{SpanSerde, StitchedSpanSerde}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.StateListener
import org.apache.kafka.streams.processor.TopologyBuilder
import org.slf4j.LoggerFactory

class StreamTopology(kafkaConfig: KafkaConfiguration, spanConfig: SpanConfiguration) extends StateListener with Thread.UncaughtExceptionHandler {

  private val LOGGER = LoggerFactory.getLogger(classOf[StreamTopology])
  private val TOPOLOGY_SOURCE_NAME = "span-source"
  private val TOPOLOGY_SINK_NAME = "stitch-span-sink"
  private val TOPOLOGY_STITCH_SPAN_PROCESSOR_NAME = "span-stitching-process"

  /**
    * builds the topology and start kstreams
    */
  def start(): KafkaStreams = {
    val streams = new KafkaStreams(topology(), kafkaConfig.streamsConfig)
    streams.setStateListener(this)
    streams.setUncaughtExceptionHandler(this)
    streams
  }

  private def topology(): TopologyBuilder = {
    val builder = new TopologyBuilder()

    builder.addSource(
      kafkaConfig.autoOffsetReset,
      TOPOLOGY_SOURCE_NAME,
      new ByteArrayDeserializer,
      SpanSerde.deserializer,
      kafkaConfig.consumeTopic)

    builder.addProcessor(
      TOPOLOGY_STITCH_SPAN_PROCESSOR_NAME,
      new SpanStitchProcessSupplier(spanConfig),
      TOPOLOGY_SOURCE_NAME)

    builder.addSink(
      TOPOLOGY_SINK_NAME,
      kafkaConfig.produceTopic,
      new ByteArraySerializer,
      StitchedSpanSerde.serializer,
      TOPOLOGY_STITCH_SPAN_PROCESSOR_NAME)

    builder
  }

  /**
    * on change event of kafka streams
    * @param newState new state of kafka streams
    * @param oldState old state of kafka streams
    */
  override def onChange (newState: KafkaStreams.State, oldState: KafkaStreams.State): Unit = {
    LOGGER.info(s"State change event called with newState=$newState and oldState=$oldState")
  }

  /**
    *
    * @param t thread which raises the exception
    * @param e throwable object
    */
  override def uncaughtException(t: Thread, e: Throwable): Unit = {
    LOGGER.error(s"uncaught exception occurred running kafka streams for thread=${t.getName}", e)
  }
}
