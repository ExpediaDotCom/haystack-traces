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

package com.expedia.www.haystack.stitch.span.collector.writers.es.index.document

import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.stitch.span.collector.config.entities.IndexConfiguration
import com.expedia.www.haystack.stitch.span.collector.writers.es.index.document.Document.{TagKey, TagValue}
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Random, Success, Try}

class IndexDocumentGenerator(config: IndexConfiguration) {
  private val RANDOM_ID_LENGTH = 4

  def createIndexDocument(stitchedSpan: StitchedSpan): Option[Document] = {
    val spanIndices = for(sp <- stitchedSpan.getChildSpansList; if isValidForIndex(sp)) yield transform(sp)
    val docId = s"${stitchedSpan.getTraceId}_${Random.alphanumeric.take(RANDOM_ID_LENGTH).mkString}"
    if (spanIndices.nonEmpty) Some(Document(docId, StitchedSpanIndex(0, spanIndices))) else None
  }

  private def isValidForIndex(span: Span): Boolean = {
    span.getProcess != null &&
      StringUtils.isNotEmpty(span.getProcess.getServiceName) &&
      StringUtils.isNotEmpty(span.getOperationName)
  }

  /**
    * transforms a span object into a map of key-value pairs for indexing
    * @param span a span object
    * @return map of key-values
    */
  private def transform(span: Span): SpanIndex = {
    // index the tags that are configured to be indexed.
    val indexedTags = mutable.Map[TagKey, TagValue]()

    for (tag <- span.getTagsList;
         indexField = config.indexableTagsByTagName.get(tag.getKey); if indexField.isDefined && indexField.get.enabled;
         (k, v) = convertToKeyValue(tag);
         convertedToIndexFieldType = convertValueToIndexFieldType(indexField.get.`type`, v); if convertedToIndexFieldType.isDefined) {
      indexedTags.put(k, convertedToIndexFieldType.get)
    }

    SpanIndex(span.getProcess.getServiceName, span.getOperationName, span.getDuration, indexedTags)
  }

  private def convertValueToIndexFieldType(fieldType: String, value: TagValue): Option[TagValue] = {
    Try (fieldType match {
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

  private def convertToKeyValue(tag: Tag): (TagKey, TagValue) = {
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
