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
import org.apache.commons.lang3.StringUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class SpansDoc(spans: Seq[Map[String, Any]])
case class IndexDocument(doc: SpansDoc, upsert: SpansDoc)

class SpanIndexDocumentGenerator(config: IndexConfiguration) {
  protected implicit val formats = DefaultFormats

  def create(spans: Seq[Span]): Option[String] = {
    val doc = for (sp <- spans; result = transform(sp); if result.nonEmpty) yield result
    if (doc.nonEmpty) Some(Serialization.write(IndexDocument(SpansDoc(doc), SpansDoc(doc)))) else None
  }

  /**
    * transforms a span object into a map of key-value pairs for indexing
    * @param span a span object
    * @return map of key-values
    */
  private def transform(span: Span): Map[String, Any] = {
    val indexMap = mutable.Map[String, Any]()

    // add service, operation names and duration as default indexing fields
    if(config.serviceField.enabled && span.getProcess != null && StringUtils.isNotEmpty(span.getProcess.getServiceName)) {
      indexMap.put(config.serviceField.name, span.getProcess.getServiceName)
    }
    if(config.operationField.enabled && StringUtils.isNotEmpty(span.getOperationName)) {
      indexMap.put(config.operationField.name, span.getOperationName)
    }
    if(config.durationField.enabled) {
      indexMap.put(config.durationField.name, span.getDuration)
    }

    // index the tags that are configured to be indexed.
    for(tag <- span.getTagsList;
        indexField = config.tags.get(tag.getKey); if indexField.isDefined;
        kvPair = convertToKeyValue(tag);
        convertedToIndexingType = convertValueToIndexAttrType(indexField.get.`type`, kvPair._2); if convertedToIndexingType.isDefined) {
      indexMap.put(kvPair._1, convertedToIndexingType.get)
    }

    indexMap.toMap
  }

  private def convertValueToIndexAttrType(attrType: String, value: Any): Option[Any] = {
    Try (attrType match {
      case "string" => value.toString
      case "long" | "int" => value.toString.toLong
      case "bool" => value.toString.toBoolean
      case "double" => value.toString.toDouble
      case _ => value
    }) match {
      case Success(result) => Some(result)
      case Failure(_) =>
        //TODO: should log this? wondering if input is crazy, then we might end up logging too many errors
        None
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
