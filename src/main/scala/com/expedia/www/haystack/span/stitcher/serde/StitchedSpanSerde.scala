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

import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.span.stitcher.metrics.MetricsSupport

object StitchedSpanSerde extends AbstractSerde[StitchedSpan] with MetricsSupport {

  private val deserFailure = metricRegistry.meter("stitch.span.deser.failure")

  /**
    * converts the binary protobuf bytes into StitchedSpan object
    * @param data serialized bytes of StitchedSpan
    * @return
    */
  override def performDeserialize(data: Array[Byte]): StitchedSpan = {
    try {
      StitchedSpan.parseFrom(data)
    } catch {
      case _: Exception =>
        /* may be log and add metric */
        deserFailure.mark()
        null
    }
  }
}
