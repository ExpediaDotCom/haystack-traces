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

import java.util.Properties

import com.expedia.www.haystack.span.stitcher.config.KafkaStreamsConfiguration
import com.expedia.www.haystack.span.stitcher.serde.{SpanSerde, StitchedSpanSerde}
import com.expedia.www.haystack.span.stitcher.transformers.SpanStitchProcessSupplier
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams.StateListener
import org.apache.kafka.streams.processor.{TopologyBuilder, UsePreviousTimeOnInvalidTimestamp}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory

class StreamTopology(config: KafkaStreamsConfiguration) extends StateListener with Thread.UncaughtExceptionHandler {

  private val LOGGER = LoggerFactory.getLogger(classOf[StreamTopology])
  private val TOPOLOGY_SOURCE_NAME = "span-source"
  private val TOPOLOGY_SINK_NAME = "stitch-span-sink"
  private val TOPOLOGY_STITCH_SPAN_PROCESSOR_NAME = "span-stitching-process"

  /**
    * builds the topology and start kstreams
    */
  def start(): KafkaStreams = {
    val streams = new KafkaStreams(topology(), streamsConfig())
    streams.setStateListener(this)
    streams.setUncaughtExceptionHandler(this)
    streams
  }

  private def topology(): TopologyBuilder = {
    val builder = new TopologyBuilder()

    builder.addSource(
      config.offsetResetPolicy,
      TOPOLOGY_SOURCE_NAME,
      Serdes.ByteArray().deserializer(),
      SpanSerde.deserializer(),
      config.consumerConfig.topic)

    builder.addProcessor(
      TOPOLOGY_STITCH_SPAN_PROCESSOR_NAME,
      new SpanStitchProcessSupplier(config),
      TOPOLOGY_SOURCE_NAME)

    builder.addSink(
      TOPOLOGY_SINK_NAME,
      config.producerConfig.topic,
      Serdes.ByteArray().serializer(),
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

  /**
    *
    * @return streams configuration object
    */
  private def streamsConfig() = {
    import StreamsConfig._

    val props = new Properties
    props.put(APPLICATION_ID_CONFIG, config.applicationId)
    props.put(BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
    props.put(NUM_STREAM_THREADS_CONFIG, config.numStreamThreads.toString)
    props.put(COMMIT_INTERVAL_MS_CONFIG, config.commitIntervalMillis.toString)
    props.put(TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[UsePreviousTimeOnInvalidTimestamp].getName)
    props.put(METADATA_MAX_AGE_CONFIG, config.metadataMaxAgeMs.toString)
    props.put(RETRY_BACKOFF_MS_CONFIG, config.retryBackoffMillis.toString)
    props.put(RECONNECT_BACKOFF_MS_CONFIG, config.reconnectBackOffMillis.toString)

    // producer specific properties
    props.put(producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), config.producerConfig.batchSize.toString)
    props.put(producerPrefix(ProducerConfig.BUFFER_MEMORY_CONFIG), config.producerConfig.maxBufferSize.toString)
    props.put(producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), config.producerConfig.maxBlockMillis.toString)
    props.put(producerPrefix(ProducerConfig.RETRIES_CONFIG), config.producerConfig.retries.toString)

    // consumer specific properties
    new StreamsConfig(props)
  }
}
