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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.expedia.www.haystack.trace.indexer.config.entities._
import com.expedia.www.haystack.trace.indexer.processors.supplier.{SpanAccumulateProcessSupplier, TraceWriteProcessorSupplier}
import com.expedia.www.haystack.trace.indexer.serde.SpanSerde
import com.expedia.www.haystack.trace.indexer.store.SpanBufferMemoryStoreSupplier
import com.expedia.www.haystack.trace.indexer.writers.cassandra.CassandraWriter
import com.expedia.www.haystack.trace.indexer.writers.es.ElasticSearchWriter
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.StateListener
import org.apache.kafka.streams.processor.TopologyBuilder
import org.slf4j.LoggerFactory

object StreamTopology {
  val STATE_STORE_NAME = "TraceSpanBufferStore"
}

class StreamTopology(kafkaConfig: KafkaConfiguration,
                     spanAccumulatorConfig: SpanAccumulatorConfiguration,
                     esConfig: ElasticSearchConfiguration,
                     cassandraConfig: CassandraConfiguration,
                     indexConfig: IndexConfiguration)
  extends StateListener with Thread.UncaughtExceptionHandler with AutoCloseable {

  implicit private val executor = scala.concurrent.ExecutionContext.Implicits.global

  private val LOGGER = LoggerFactory.getLogger(classOf[StreamTopology])
  private val TOPOLOGY_SOURCE_NAME = "span-topic-source"
  private val TOPOLOGY_SINK_NAME = "span-buffer-topic-sink"
  private val TOPOLOGY_BUFFERED_SPAN_PROCESSOR_NAME = "span-accumulation-process"
  private val TOPOLOGY_WRITE_TRACE_PROCESSOR_NAME = "trace-writer-process"

  private var streams: KafkaStreams = _
  private val running = new AtomicBoolean(false)

  private val cassandraWriter = new CassandraWriter(cassandraConfig)(executor)
  private val elasticSearchWriter = new ElasticSearchWriter(esConfig, indexConfig)

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
    val topologyBuilder = createKafkaSource()
    addSpanBufferProcessorWithStateStore(topologyBuilder)
    addTraceWriterProcessor(topologyBuilder)
    addKafkaSink(topologyBuilder)
    topologyBuilder
  }

  private def createKafkaSource(): TopologyBuilder = {
    val builder = new TopologyBuilder()
    builder.addSource(
      kafkaConfig.autoOffsetReset,
      TOPOLOGY_SOURCE_NAME,
      kafkaConfig.timestampExtractor,
      new StringDeserializer,
      new SpanSerde().deserializer,
      kafkaConfig.consumeTopic)
  }

  private def addSpanBufferProcessorWithStateStore(builder: TopologyBuilder): Unit = {
    builder.addProcessor(
      TOPOLOGY_BUFFERED_SPAN_PROCESSOR_NAME,
      new SpanAccumulateProcessSupplier(spanAccumulatorConfig),
      TOPOLOGY_SOURCE_NAME)

    // add the state store
    val storeSupplier = new SpanBufferMemoryStoreSupplier(
      spanAccumulatorConfig.minTracesPerCache,
      spanAccumulatorConfig.maxEntriesAllStores,
      StreamTopology.STATE_STORE_NAME,
      kafkaConfig.changelogConfig.enabled,
      kafkaConfig.changelogConfig.logConfig)

    builder.addStateStore(storeSupplier, TOPOLOGY_BUFFERED_SPAN_PROCESSOR_NAME)
  }

  private def addTraceWriterProcessor(builder: TopologyBuilder): Unit = {
    builder.addProcessor(
      TOPOLOGY_WRITE_TRACE_PROCESSOR_NAME,
      new TraceWriteProcessorSupplier(cassandraWriter, elasticSearchWriter),
      TOPOLOGY_BUFFERED_SPAN_PROCESSOR_NAME)
  }

  private def addKafkaSink(builder: TopologyBuilder): Unit = {
    builder.addSink(
      TOPOLOGY_SINK_NAME,
      kafkaConfig.produceTopic,
      new StringSerializer,
      new ByteArraySerializer,
      TOPOLOGY_WRITE_TRACE_PROCESSOR_NAME)
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
    // TODO: need to verify if this is a good design practice to restart the stream all over again?
    if (closeStream()) {
      start() // start all over again
    }
  }

  /**
    * close the stream if it is not running,
    * @return
    */
  private[haystack] def closeStream(): Boolean = {
    if(running.getAndSet(false)) {
      LOGGER.info("Closing the kafka streams.")
      try {
        streams.close(kafkaConfig.streamsCloseTimeoutInMillis, TimeUnit.MILLISECONDS)
      } catch {
        case ex: Exception => LOGGER.error("Fail to close the kafka streams with reason", ex)
      }
      true
    } else {
      false
    }
  }

  override def close(): Unit = {
    closeStream()
    elasticSearchWriter.close()
    cassandraWriter.close()
  }
}
