package com.expedia.www.haystack.trace.commons.clients.es.document

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.mutable

object Document {
  implicit val formats = DefaultFormats
  type TagKey = String
  type TagValue = Any
}

case class Document(id: String, doc: TraceIndexDoc) {
  val json: String = Serialization.write(doc)(Document.formats)
}

case class TraceIndexDoc(rootDuration: Long, spans: Seq[mutable.Map[String, Any]])
