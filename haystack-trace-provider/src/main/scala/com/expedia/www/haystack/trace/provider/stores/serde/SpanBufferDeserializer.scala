/*
 *  Copyright 2017 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.expedia.www.haystack.trace.provider.stores.serde

import com.codahale.metrics.Meter
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.provider.metrics.MetricsSupport

object SpanBufferDeserializer extends MetricsSupport {
  protected val deserFailure: Meter = metricRegistry.meter("span.buffer.deser.failure")
}

class SpanBufferDeserializer  {
  def deserialize(data: Array[Byte]): SpanBuffer = {
    try {
      if(data == null || data.length > 0) SpanBuffer.parseFrom(data) else null
    } catch {
      case _: Exception =>
        SpanBufferDeserializer.deserFailure.mark()
        null
    }
  }
}