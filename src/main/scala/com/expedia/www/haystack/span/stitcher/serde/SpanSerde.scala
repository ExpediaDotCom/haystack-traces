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
package com.expedia.www.haystack.span.stitcher.serde

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.span.stitcher.metrics.AppMetricNames._
import com.expedia.www.haystack.span.stitcher.metrics.MetricsSupport

object SpanSerde extends AbstractSerde[Span] with MetricsSupport {

  private val spanDeserMeter = metricRegistry.meter(SPAN_DESER_FAILURE)

  /**
    * converts the binary protobuf bytes into Span object
    * @param data serialized bytes of Span
    * @return
    */
  override def performDeserialize(data: Array[Byte]): Span = {
    try {
      Span.parseFrom(data)
    } catch {
      case _: Exception =>
        /* may be log and add metric */
        spanDeserMeter.mark()
        null
    }
  }
}
