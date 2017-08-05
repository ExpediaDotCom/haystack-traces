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
package com.expedia.www.haystack.span.stitcher.integration.serdes

import java.util

import com.expedia.open.tracing.Span
import com.expedia.open.tracing.stitch.StitchedSpan
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

class SpanSerializer extends Serializer[Span] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
  override def serialize(topic: String, data: Span): Array[Byte] = {
    data.toByteArray
  }
  override def close(): Unit = ()
}

class StitchSpanDeserializer extends Deserializer[StitchedSpan] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): StitchedSpan = {
    if(data == null) null else StitchedSpan.parseFrom(data)
  }

  override def close(): Unit = ()
}