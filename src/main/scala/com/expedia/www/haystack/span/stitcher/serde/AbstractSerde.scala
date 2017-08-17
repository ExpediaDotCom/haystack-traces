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

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import com.google.protobuf.GeneratedMessageV3

abstract class AbstractSerde[T <: GeneratedMessageV3] extends Serde[T] {

  def performDeserialize(data: Array[Byte]): T

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()

  def serializer: Serializer[T] = {
    new Serializer[T] {
      override def configure(configs: util.Map[String, _], b: Boolean): Unit = ()

      override def close(): Unit = ()

      override def serialize(topic: String, obj: T): Array[Byte] = if(obj != null) obj.toByteArray else null
    }
  }

  def deserializer: Deserializer[T] = {
    new Deserializer[T] {
      override def configure(configs: util.Map[String, _], b: Boolean): Unit = ()

      override def close(): Unit = ()

      override def deserialize(topic: String, data: Array[Byte]): T = performDeserialize(data)
    }
  }
}
