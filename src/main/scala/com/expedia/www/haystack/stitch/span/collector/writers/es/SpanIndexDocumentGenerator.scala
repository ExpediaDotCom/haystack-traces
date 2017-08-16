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

import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.stitch.span.collector.config.entities.IndexConfiguration
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.JavaConversions._
import scala.collection.mutable

case class IndexDocument(doc: Seq[Map[String, Any]], upsert: Seq[Map[String, Any]])

class SpanIndexDocumentGenerator(config: IndexConfiguration) {
  protected implicit val formats = DefaultFormats

  def create(spans: Seq[Span]): String = {
    val doc = for(sp <- spans) yield transform(sp)
    Serialization.write(IndexDocument(doc, doc))
  }

  private def transform(span: Span): Map[String, Any] = {
    // add service and operation names as default indexing keys
    val indexMap = mutable.Map[String, Any](
      config.serviceFieldName -> span.getProcess.getServiceName,
      config.operationFieldName -> span.getOperationName,
      config.durationFieldName -> span.getDuration)

    span.getTagsList
      .foreach { tag =>
        config.indexableTags.get(tag.getKey) match {
          case Some(attr) =>
            val tagKeyValue = convertToKeyValue(tag)
            indexMap.put(tagKeyValue._1, convertValueToIndexAttrType(attr.`type`, tagKeyValue._2))
          case _ =>
        }
      }

    indexMap.toMap
  }

  private def convertValueToIndexAttrType(attrType: String, value: Any): Any = {
    attrType match {
      case "string" => value.toString
      case "long" => value.toString.toLong
      case "bool" => value.toString.toBoolean
      case "double" => value.toString.toDouble
      case _ => value
    }
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
