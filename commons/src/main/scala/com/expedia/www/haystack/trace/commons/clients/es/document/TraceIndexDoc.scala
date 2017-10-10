package com.expedia.www.haystack.trace.commons.clients.es.document

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.mutable

object TraceIndexDoc {
  implicit val formats = DefaultFormats
  type TagKey = String
  type TagValue = Any
}

case class TraceIndexDoc(traceid: String, rootDuration: Long, spans: Seq[mutable.Map[String, Any]]) {
  val json: String = Serialization.write(this)(TraceIndexDoc.formats)
}
