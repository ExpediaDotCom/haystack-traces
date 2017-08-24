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
package com.expedia.www.haystack.span.bufferer.processors

import com.expedia.open.tracing.Span
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.span.bufferer.config.entities.SpanBufferConfiguration
import com.expedia.www.haystack.span.bufferer.metrics.AppMetricNames._
import com.expedia.www.haystack.span.bufferer.metrics.MetricsSupport

class MeteredSpanBufferingProcessor(config: SpanBufferConfiguration) extends SpanBufferingProcessor(config)
  with MetricsSupport {

  private val punctuateTimer = metricRegistry.timer(PUNCTUATE_TIMER)
  private val processTimer = metricRegistry.timer(PROCESS_TIMER)
  private val bufferedSpansHistogram = metricRegistry.histogram(BUFFERED_SPANS_COUNT)

  override def punctuate(timestamp: Long): Unit = {
    val timer = punctuateTimer.time()
    super.punctuate(timestamp)
    timer.stop()
  }

  override def process(key: String, span: Span): Unit = {
    val timer = processTimer.time()
    super.process(key, span)
    timer.stop()
  }

  override def forward(key: String, spanBuffer: SpanBuffer): Unit = {
    bufferedSpansHistogram.update(spanBuffer.getChildSpansCount)
    super.forward(key, spanBuffer)
  }
}
