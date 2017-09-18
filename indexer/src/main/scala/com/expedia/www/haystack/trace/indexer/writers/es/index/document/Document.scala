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

package com.expedia.www.haystack.trace.indexer.writers.es.index.document

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.mutable

object Document {
  implicit val formats = DefaultFormats
  type TagKey = String
  type TagValue = Any
}

//case class SpanIndexDoc(spanId: String,
//                        service: String,
//                        operation: String,
//                        duration: Long,
//                        tags: mutable.Map[String, mutable.ListBuffer[Any]])

case class TraceIndexDoc(rootDuration: Long, spans: Seq[mutable.Map[String, Any]])

case class Document(id: String, doc: TraceIndexDoc) {
  val json: String = Serialization.write(doc)(Document.formats)
}
