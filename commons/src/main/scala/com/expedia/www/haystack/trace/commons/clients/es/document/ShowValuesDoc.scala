package com.expedia.www.haystack.trace.commons.clients.es.document

import org.json4s.jackson.Serialization

case class ShowValuesDoc(servicename: String,
                              fieldname: String,
                              fieldvalue: String) {
  val json: String = Serialization.write(this)(TraceIndexDoc.formats)
}

