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
import com.expedia.www.haystack.span.stitcher.metrics.MetricsSupport
import com.expedia.www.haystack.span.stitcher.metrics.AppMetricNames._

class MeteredSpanStitchProcessor(stitchConfig: StitchConfiguration) extends SpanStitchProcessor(stitchConfig)
  with MetricsSupport {

  private val punctuateTimer = metricRegistry.timer(STITCH_PUNCTUATE_TIMER)
  private val stitchProcessTimer = metricRegistry.timer(STITCH_PROCESS_TIMER)
  private val stitchedSpansHistogram = metricRegistry.histogram(STITCH_SPAN_COUNT)

  override def punctuate(timestamp: Long): Unit = {
    val timer = punctuateTimer.time()
    super.punctuate(timestamp)
    timer.stop()
  }

  override def process(key: String, span: Span): Unit = {
    val timer = stitchProcessTimer.time()
    super.process(key, span)
    timer.stop()
  }

  override def forward(key: String, stitchedSpan: StitchedSpan): Unit = {
    stitchedSpansHistogram.update(stitchedSpan.getChildSpansCount)
    super.forward(key, stitchedSpan)
  }
}
