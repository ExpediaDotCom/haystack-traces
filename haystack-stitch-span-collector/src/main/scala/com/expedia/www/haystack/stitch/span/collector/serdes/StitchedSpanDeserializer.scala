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

package com.expedia.www.haystack.stitch.span.collector.serdes

import com.codahale.metrics.Meter
import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.stitch.span.collector.metrics.MetricsSupport

object StitchedSpanDeserializer extends MetricsSupport {
  protected val deserFailure: Meter = metricRegistry.meter("stitched.span.deser.failure")
}

class StitchedSpanDeserializer  {

  /**
    * deserialize the data bytes to StitchedSpan proto object
    * @param data serialized bytes
    * @return a deserialized stitched span object
    */
  def deserialize(data: Array[Byte]): StitchedSpan = {
    try {
      if(data.length > 0) {
        StitchedSpan.parseFrom(data)
      } else {
        null
      }
    } catch {
      case _: Exception =>
        StitchedSpanDeserializer.deserFailure.mark()
        null
    }
  }
}
