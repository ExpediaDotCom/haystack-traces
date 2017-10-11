package com.expedia.www.haystack.trace.commons.clients.es.document

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.mutable

object TraceIndexDoc {
  implicit val formats = DefaultFormats
  type TagKey = String
  type TagValue = Any

  val SERVICE_KEY_NAME = "service"
  val OPERATION_KEY_NAME = "operation"
  val DURATION_KEY_NAME = "duration"
  val ROOT_DURATION_KEY_NAME = "rootduration"
  val START_TIME_KEY_NAME = "starttime"
  val SPAN_ID_KEY_NAME = "spanid"
}

case class TraceIndexDoc(traceid: String, rootDuration: Long, spans: Seq[mutable.Map[String, Any]]) {
  val json: String = Serialization.write(this)(TraceIndexDoc.formats)
}
