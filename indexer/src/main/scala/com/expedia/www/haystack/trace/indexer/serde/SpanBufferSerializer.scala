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

import java.util

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.indexer.metrics.AppMetricNames._
import com.expedia.www.haystack.trace.indexer.metrics.MetricsSupport
import org.apache.kafka.common.serialization.Serializer


class SpanBufferSerializer extends Serializer[SpanBuffer] with MetricsSupport {

  private val deserFailure = metricRegistry.meter(SPAN_BUFFER_PROTO_DESER_FAILURE)

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  /**
    * converts the SpanBuffer object to serialized bytes
    * @param data SpanBuffer
    * @return
    */
  override def serialize(topic: String, data: SpanBuffer): Array[Byte] = {
    try {
      data.toByteArray
    } catch {
      case _: Exception =>
        /* may be log and add metric */
        deserFailure.mark()
        null
    }
  }

  override def close(): Unit = ()
}
