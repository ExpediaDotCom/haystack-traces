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

import java.util

import com.expedia.open.tracing.stitch.StitchedSpan
import org.apache.kafka.common.serialization.Deserializer

class StitchedSpanDeserializer extends Deserializer[StitchedSpan] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()

  /**
    * deserialize the data bytes to StitchedSpan proto object
    * @param topic the input topic
    * @param data serialized bytes
    * @return
    */
  override def deserialize(topic: String, data: Array[Byte]): StitchedSpan = {
    try {
      StitchedSpan.parseFrom(data)
    } catch {
      case _: Exception =>
        null
    }
  }
}
