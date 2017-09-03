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

import java.util.Objects

import com.expedia.open.tracing.Span
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.indexer.StreamTopology
import com.expedia.www.haystack.trace.indexer.config.entities.SpanAccumulatorConfiguration
import com.expedia.www.haystack.trace.indexer.store.data.model.SpanBufferWithMetadata
import com.expedia.www.haystack.trace.indexer.store.traits.{EldestBufferedSpanEvictionListener, SpanBufferKeyValueStore}
import org.apache.kafka.streams.processor.{Processor, ProcessorContext}

import scala.collection.JavaConversions._

class SpanAccumulateProcessor(config: SpanAccumulatorConfiguration) extends Processor[String, Span]
  with EldestBufferedSpanEvictionListener {

  private var context: ProcessorContext = _
  private var store: SpanBufferKeyValueStore = _

  /**
    * initializes the span buffering processor
    *
    * @param context processor context object
    */
  override def init(context: ProcessorContext): Unit = {
    this.context = context
    this.context.schedule(config.pollIntervalMillis)

    this.store = context.getStateStore(StreamTopology.STATE_STORE_NAME).asInstanceOf[SpanBufferKeyValueStore]
    require(this.store != null, "State store can't be null")
    this.store.addEvictionListener(this)
  }

  /**
    *
    * @param timestamp the stream's current timestamp.
    */
  override def punctuate(timestamp: Long): Unit = {
    this.store.getAndRemoveSpanBuffersOlderThan(timestamp - config.bufferingWindowMillis) foreach {
      case (traceId, spanBuffer) => forward(traceId, spanBuffer.builder.build())
    }
  }

  /**
    * finds the span-buffer object in state store using span's traceId. If not found,
    * creates a new one, else adds to the child spans
    *
    * @param traceId  for spans, partition key always be its traceId
    * @param span     span object
    */
  override def process(traceId: String, span: Span): Unit = {
    if (span != null) {
     this.store.addOrUpdateSpanBuffer(traceId, span, this.context.timestamp())
    }
  }

  /**
    * close the processor
    */
  override def close(): Unit = ()

  /**
    * this is called when the store evicts the oldest record due to size constraints,
    * we forward this record to next processor
    *
    * @param traceId   partition key of the spanBuffer ie traceId
    * @param value     span-buffer protobuf builder
    */
  override def onEvict(traceId: String, value: SpanBufferWithMetadata): Unit = {
    forward(traceId, value.builder.build())
  }

  /**
    * forwards the span buffer to next processor/sink
    * @param traceId partition key of the spanBuffer ie traceId
    * @param spanBuffer span buffer proto object
    */
  protected def forward(traceId: String, spanBuffer: SpanBuffer): Unit = {
    this.context.forward(traceId, spanBuffer)
  }
}
