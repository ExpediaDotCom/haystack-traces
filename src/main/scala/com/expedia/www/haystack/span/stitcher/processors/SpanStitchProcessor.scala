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
package com.expedia.www.haystack.span.stitcher.processors

import com.expedia.open.tracing.Span
import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.span.stitcher.config.entities.StitchConfiguration
import com.expedia.www.haystack.span.stitcher.store.data.model.StitchedSpanWithMetadata
import com.expedia.www.haystack.span.stitcher.store.traits.{EldestStitchedSpanEvictionListener, StitchedSpanKVStore}
import org.apache.kafka.streams.processor.{Processor, ProcessorContext}

import scala.collection.JavaConversions._

class SpanStitchProcessor(stitchConfig: StitchConfiguration) extends Processor[String, Span]
  with EldestStitchedSpanEvictionListener {

  private var context: ProcessorContext = _
  private var store: StitchedSpanKVStore = _

  /**
    * initializes the span stitch processor
    *
    * @param context processor context object
    */
  override def init(context: ProcessorContext): Unit = {
    this.context = context
    this.store = context.getStateStore("StitchedSpanStore").asInstanceOf[StitchedSpanKVStore]

    this.store.addEvictionListener(this)
    this.context.schedule(stitchConfig.pollIntervalMillis)
  }

  /**
    *
    * @param timestamp the stream's current timestamp.
    */
  override def punctuate(timestamp: Long): Unit = {
    this.store.getAndRemoveSpansInWindow(timestamp, stitchConfig.stitchWindowMillis) foreach {
      case (key, value) =>
        val stitchedSpan = value.builder.build()
        context.forward(key, stitchedSpan)
    }
  }

  /**
    * finds the stitched-span object in state store using span's traceId. If not found,
    * creates a new one, else adds to the child spans
    *
    * @param key  for spans, partition key always be its traceId
    * @param span span object
    */
  override def process(key: String, span: Span): Unit = {
    if (span != null) {
      val value = this.store.get(key)
      if (value == null) {
        val stitchSpanBuilder = StitchedSpan.newBuilder().setTraceId(span.getTraceId).addChildSpans(span)
        this.store.put(key, StitchedSpanWithMetadata(stitchSpanBuilder, this.context.timestamp()))
      } else {
        // add this span as a child span to existing builder
        value.builder.addChildSpans(span)
      }
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
    * @param key   partition key of the stitched span ie traceId
    * @param value stiched span protobuf builder
    */
  override def onEvict(key: String, value: StitchedSpanWithMetadata): Unit = {
    this.context.forward(key, value.builder.build())
  }
}
