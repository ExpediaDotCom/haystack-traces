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

package com.expedia.www.haystack.trace.indexer.processors

import com.codahale.metrics.{Histogram, Timer}
import com.expedia.open.tracing.Span
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trace.commons.packer.Packer
import com.expedia.www.haystack.trace.indexer.config.entities.SpanAccumulatorConfiguration
import com.expedia.www.haystack.trace.indexer.metrics.AppMetricNames.{BUFFERED_SPANS_COUNT, PROCESS_TIMER}
import com.expedia.www.haystack.trace.indexer.store.SpanBufferMemoryStoreSupplier
import com.expedia.www.haystack.trace.indexer.store.data.model.SpanBufferWithMetadata
import com.expedia.www.haystack.trace.indexer.store.traits.{EldestBufferedSpanEvictionListener, SpanBufferKeyValueStore}
import com.expedia.www.haystack.trace.indexer.writers.TraceWriter
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContextExecutor

object SpanIndexProcessor extends MetricsSupport {
  protected val LOGGER: Logger = LoggerFactory.getLogger(SpanIndexProcessor.getClass)

  protected val processTimer: Timer = metricRegistry.timer(PROCESS_TIMER)
  protected val bufferedSpansHistogram: Histogram = metricRegistry.histogram(BUFFERED_SPANS_COUNT)
}

class SpanIndexProcessor(accumulatorConfig: SpanAccumulatorConfiguration,
                         storeSupplier: SpanBufferMemoryStoreSupplier,
                         writers: Seq[TraceWriter],
                         spanBufferPacker: Packer[SpanBuffer])(implicit val dispatcher: ExecutionContextExecutor)
  extends StreamProcessor[String, Span] with EldestBufferedSpanEvictionListener {

  import com.expedia.www.haystack.trace.indexer.processors.SpanIndexProcessor._

  private var spanBufferMemStore: SpanBufferKeyValueStore = _

  // defines the last time we look into the store for emitting the traces
  private var lastEmitTimestamp: Long = 0L

  override def init(): Unit = {
    spanBufferMemStore = storeSupplier.get()
    spanBufferMemStore.init()
    spanBufferMemStore.addEvictionListener(this)
    LOGGER.info("Span Index Processor has been initialized successfully!")
  }

  override def close(): Unit = {
    spanBufferMemStore.close()
    LOGGER.info("Span Index Processor has been closed now!")
  }

  override def process(records: Iterable[ConsumerRecord[String, Span]]): Option[OffsetAndMetadata] = {
    val timer = processTimer.time()
    try {
      var currentTimestamp = 0L
      records
        .filter(_ != null)
        .foreach {
          record => {
            spanBufferMemStore.addOrUpdateSpanBuffer(record.key(), record.value(), record.timestamp(), record.offset())
            currentTimestamp = Math.max(record.timestamp(), currentTimestamp)
          }
        }

      mayBeEmit(currentTimestamp)
    } finally {
      timer.stop()
    }
  }

  private def writeTrace(spanBuffer: SpanBuffer, isLastSpanBuffer: Boolean) = {
    // get a metric on spans that are buffered before we write them to external databases
    bufferedSpansHistogram.update(spanBuffer.getChildSpansCount)

    val traceId = spanBuffer.getTraceId
    val packedMessage = spanBufferPacker.apply(spanBuffer)
    writers.foreach {
      writer =>
        writer.writeAsync(traceId, packedMessage, isLastSpanBuffer)
    }
  }

  private def mayBeEmit(currentTimestamp: Long): Option[OffsetAndMetadata] = {
    if ((currentTimestamp - accumulatorConfig.pollIntervalMillis) > lastEmitTimestamp) {

      val forceEvictionTimestamp = currentTimestamp - accumulatorConfig.maxSpanBufferEmitWindowMillis
      val completedSpanBufferEvictionTimeout = currentTimestamp - accumulatorConfig.completedSpanBufferEmitWindow
      val emittableSpanBuffersWithOffset = spanBufferMemStore.getAndRemoveSpanBuffersOlderThan(forceEvictionTimestamp, completedSpanBufferEvictionTimeout)

      emittableSpanBuffersWithOffset.spanBuffers.zipWithIndex foreach {
        case (sb, idx) =>
          val spanBuffer = sb.builder.build()
          writeTrace(spanBuffer, idx == emittableSpanBuffersWithOffset.spanBuffers.size - 1)
      }
      lastEmitTimestamp = currentTimestamp
      emittableSpanBuffersWithOffset.kafkaOffset
    } else {
      None
    }
  }

  // for now we set islastSpanBuffer as false.
  // if too many eviction happens, then writer will flush it out eventually
  override def onEvict(key: String, value: SpanBufferWithMetadata): Unit = writeTrace(value.builder.build(), isLastSpanBuffer = false)
}
