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

package com.expedia.www.haystack.span.bufferer

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.expedia.www.haystack.span.bufferer.config.entities.{KafkaConfiguration, SpanBufferConfiguration}
import com.expedia.www.haystack.span.bufferer.processors.SpanBufferProcessSupplier
import com.expedia.www.haystack.span.bufferer.serde.{SpanSerde, SpanBufferSerde}
import com.expedia.www.haystack.span.bufferer.store.BufferedSpanMemStoreSupplier
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.StateListener
import org.apache.kafka.streams.processor.TopologyBuilder
import org.slf4j.LoggerFactory

object StreamTopology {
  val STATE_STORE_NAME = "BufferedSpanStore"
}

class StreamTopology(kafkaConfig: KafkaConfiguration,
                     spanBufferConfig: SpanBufferConfiguration)
  extends StateListener with Thread.UncaughtExceptionHandler {

  private val LOGGER = LoggerFactory.getLogger(classOf[StreamTopology])
  private val TOPOLOGY_SOURCE_NAME = "span-source"
  private val TOPOLOGY_SINK_NAME = "buffered-span-sink"
  private val TOPOLOGY_BUFFERED_SPAN_PROCESSOR_NAME = "span-buffering-process"

  private var streams: KafkaStreams = _
  private val running = new AtomicBoolean(false)

  Runtime.getRuntime.addShutdownHook(new ShutdownHookThread)

  /**
    * builds the topology and start kstreams
    */
  def start(): Unit = {
    LOGGER.info("Starting the kafka stream topology.")

    streams = new KafkaStreams(topology(), kafkaConfig.streamsConfig)
    streams.setStateListener(this)
    streams.setUncaughtExceptionHandler(this)
    streams.cleanUp()
    streams.start()
    running.set(true)
  }

  private def topology(): TopologyBuilder = {
    val builder = new TopologyBuilder()

    builder.addSource(
      kafkaConfig.autoOffsetReset,
      TOPOLOGY_SOURCE_NAME,
      kafkaConfig.timestampExtractor,
      new StringDeserializer,
      new SpanSerde().deserializer,
      kafkaConfig.consumeTopic)

    builder.addProcessor(
      TOPOLOGY_BUFFERED_SPAN_PROCESSOR_NAME,
      new SpanBufferProcessSupplier(spanBufferConfig),
      TOPOLOGY_SOURCE_NAME)

    // add the state store
    val storeSupplier = new BufferedSpanMemStoreSupplier(
      spanBufferConfig.initialStoreSize,
      spanBufferConfig.maxEntriesAllStores,
      StreamTopology.STATE_STORE_NAME,
      kafkaConfig.changelogConfig.enabled,
      kafkaConfig.changelogConfig.logConfig)

    builder.addStateStore(storeSupplier, TOPOLOGY_BUFFERED_SPAN_PROCESSOR_NAME)

    builder.addSink(
      TOPOLOGY_SINK_NAME,
      kafkaConfig.produceTopic,
      new StringSerializer,
      new SpanBufferSerde().serializer,
      TOPOLOGY_BUFFERED_SPAN_PROCESSOR_NAME)

    builder
  }

  /**
    * on change event of kafka streams
    * @param newState new state of kafka streams
    * @param oldState old state of kafka streams
    */
  override def onChange(newState: KafkaStreams.State, oldState: KafkaStreams.State): Unit = {
    LOGGER.info(s"State change event called with newState=$newState and oldState=$oldState")
  }

  /**
    * handle the uncaught exception by closing the current streams and rerunning it
    * @param t thread which raises the exception
    * @param e throwable object
    */
  override def uncaughtException(t: Thread, e: Throwable): Unit = {
    LOGGER.error(s"uncaught exception occurred running kafka streams for thread=${t.getName}", e)
    // it may happen that uncaught exception gets called by multiple threads at the same time,
    // so we let one of them close the kafka streams and restart it
    if (close()) {
      start() // start all over again
    }
  }

  /**
    * close the stream if it is not running
    * @return
    */
  def close(): Boolean = {
    if(running.getAndSet(false)) {
      LOGGER.info("Closing the kafka streams.")
      try {
        streams.close(spanBufferConfig.streamsCloseTimeoutMillis, TimeUnit.MILLISECONDS)
      } catch {
        case ex: Exception => LOGGER.error("Fail to close the kafka streams with reason", ex)
      }
      true
    } else {
      false
    }
  }

  private class ShutdownHookThread extends Thread {
    override def run(): Unit = close()
  }
}
