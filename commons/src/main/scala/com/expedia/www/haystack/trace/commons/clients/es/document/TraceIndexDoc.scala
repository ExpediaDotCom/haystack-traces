package com.expedia.www.haystack.trace.commons.clients.es.document

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.mutable

object TraceIndexDoc {
  implicit val formats = DefaultFormats
  type TagKey = String
  type TagValue = Any

  val SERVICE_KEY_NAME = "servicename"
  val OPERATION_KEY_NAME = "operationname"
  val DURATION_KEY_NAME = "duration"
  val START_TIME_KEY_NAME = "starttime"
}

case class TraceIndexDoc(traceid: String, rootduration: Long, spans: Seq[mutable.Map[String, Any]]) {
  val json: String = Serialization.write(this)(TraceIndexDoc.formats)
}
