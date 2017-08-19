package com.expedia.www.haystack.stitch.span.collector.writers.es.index.document

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.mutable

object Document {
  implicit val formats = DefaultFormats
  type TagKey = String
  type TagValue = Any
}

case class SpanIndex(service: String, operation: String, duration: Long, tags: mutable.Map[String, Any])
case class StitchedSpanIndex(duration: Long, spans: Seq[SpanIndex])

case class Document(id: String, stitchedSpanIndex: StitchedSpanIndex) {
  val stitchedSpanIndexJson: String = Serialization.write(stitchedSpanIndex)(Document.formats)
}
