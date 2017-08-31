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

package com.expedia.www.haystack.trace.indexer.serde

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.indexer.metrics.MetricsSupport
import com.expedia.www.haystack.trace.indexer.metrics.AppMetricNames._


class SpanBufferSerde extends AbstractSerde[SpanBuffer] with MetricsSupport {

  private val deserFailure = metricRegistry.meter(SPAN_BUFFER_PROTO_DESER_FAILURE)

  /**
    * converts the binary protobuf bytes into SpanBuffer object
    * @param data serialized bytes of SpanBuffer
    * @return
    */
  override def performDeserialize(data: Array[Byte]): SpanBuffer = {
    try {
      if(data == null || data.length == 0) null else SpanBuffer.parseFrom(data)
    } catch {
      case _: Exception =>
        /* may be log and add metric */
        deserFailure.mark()
        null
    }
  }
}
