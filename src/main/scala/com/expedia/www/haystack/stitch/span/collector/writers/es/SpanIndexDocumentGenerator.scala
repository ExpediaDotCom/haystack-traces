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

package com.expedia.www.haystack.stitch.span.collector.writers.es

import java.util

import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.stitch.span.collector.config.entities.IndexConfiguration
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.JavaConversions._
import scala.collection.mutable

case class IndexDocument(doc: Map[String, Any], upsert: Map[String, Any])

class SpanIndexDocumentGenerator(config: IndexConfiguration) {
  implicit val formats = DefaultFormats

  private val SERVICE_KEY = "service"
  private val OPERATION_KEY = "operation"
  private val DURATION = "duration"

  def create(spans: util.List[Span]): String = {
    val updateDocument = for(sp <- spans;
        indexKeyValueMap = transform(sp)) yield IndexDocument(doc = indexKeyValueMap, upsert = indexKeyValueMap)

    Serialization.write(updateDocument)
  }

  private def transform(span: Span): Map[String, Any] = {
    // add service and operation names as default indexing keys
    val indexMap = mutable.Map[String, Any](
      SERVICE_KEY -> span.getProcess.getServiceName,
      OPERATION_KEY -> span.getOperationName,
      DURATION -> span.getDuration)

    span.getTagsList
      .filter(tag => config.tagKeys.contains(tag.getKey))
      .map(tag => convertToKeyValue(tag))
      .foreach( { case (k, v) => indexMap.put(k, v ) })

    indexMap.toMap
  }

  private def convertToKeyValue(tag: Tag): (String, Any) = {
    import Tag.TagType._

    val key = tag.getKey
    tag.getType match {
      case BOOL => (key, tag.getVBool)
      case STRING => (key, tag.getVStr)
      case LONG => (key, tag.getVLong)
      case DOUBLE => (key, tag.getVDouble)
      case BINARY => (key, tag.getVBytes.toStringUtf8)
      case _ => throw new RuntimeException(s"Fail to understand the span tag type ${tag.getType} !!!")
    }
  }
}
