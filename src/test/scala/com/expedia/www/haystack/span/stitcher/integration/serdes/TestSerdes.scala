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
    StitchedSpan.parseFrom(data)
  }

  override def close(): Unit = ()
}