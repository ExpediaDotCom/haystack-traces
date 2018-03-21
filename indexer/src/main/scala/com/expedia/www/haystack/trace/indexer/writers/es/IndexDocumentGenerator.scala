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

package com.expedia.www.haystack.trace.indexer.writers.es

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.trace.commons.clients.es.document.TraceIndexDoc
import com.expedia.www.haystack.trace.commons.clients.es.document.TraceIndexDoc.TagValue
import com.expedia.www.haystack.trace.commons.config.entities.WhitelistIndexFieldConfiguration
import com.expedia.www.haystack.trace.indexer.metrics.MetricsSupport
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class IndexDocumentGenerator(config: WhitelistIndexFieldConfiguration) extends MetricsSupport {

  /**
    * @param spanBuffer a span buffer object
    * @return index document that can be put in elastic search
    */
  def createIndexDocument(traceId: String, spanBuffer: SpanBuffer): Option[TraceIndexDoc] = {
    // We maintain a white list of tags that are to be indexed. The whitelist is maintained as a configuration
    // in an external database (outside this app boundary). However, the app periodically reads this whitelist config
    // and applies it to the new spans that are read.
    val spanIndices = for(sp <- spanBuffer.getChildSpansList; if isValidForIndex(sp)) yield transform(sp)
    if (spanIndices.nonEmpty) Some(TraceIndexDoc(traceId, rootDuration(spanBuffer), spanIndices)) else None
  }

  private def isValidForIndex(span: Span): Boolean = {
    StringUtils.isNotEmpty(span.getServiceName) && StringUtils.isNotEmpty(span.getOperationName)
  }

  // finds the amount of time it takes for one trace(span buffer) to complete.
  // span buffer contains all the spans for a given TraceId
  private def rootDuration(spanBuffer: SpanBuffer): Long = {
    spanBuffer.getChildSpansList
      .find(sp => sp.getParentSpanId == null)
      .map(_.getDuration)
      .getOrElse(0L)
  }

  /**
    * transforms a span object into a index document. serviceName, operationName, duration and tags(depending upon the
    * configuration) are used to create an index document.
    * @param span a span object
    * @return span index document as a map
    */
  private def transform(span: Span): mutable.Map[String, Any] = {
    val spanIndexDoc = mutable.Map[String, Any]()

    for (tag <- span.getTagsList;
         normalizedTagKey = tag.getKey.toLowerCase;
         indexField = config.indexFieldMap.get(normalizedTagKey); if indexField != null && indexField.enabled;
         v = readTagValue(tag);
         indexableValue = transformValueForIndexing(indexField.`type`, v); if indexableValue.isDefined) {
      spanIndexDoc.put(normalizedTagKey, indexableValue)
    }

    import com.expedia.www.haystack.trace.commons.clients.es.document.TraceIndexDoc._
    spanIndexDoc.put(SERVICE_KEY_NAME, span.getServiceName)
    spanIndexDoc.put(OPERATION_KEY_NAME, span.getOperationName)
    spanIndexDoc.put(DURATION_KEY_NAME, span.getDuration)
    spanIndexDoc.put(START_TIME_KEY_NAME, span.getStartTime)

    spanIndexDoc
  }


  /**
    * this method adjusts the tag's value to the indexing field type. Take an example of 'httpstatus' tag
    * that we always want to index as a 'long' type in elastic search. Now services may send this tag value as string,
    * hence in this method, we will transform the tag value to its expected type for e.g. long.
    * In case we fail to adjust the type, we ignore the tag for indexing.
    * @param fieldType expected field type that is valid for indexing
    * @param value tag value
    * @return tag value with adjusted(expected) type
    */
  private def transformValueForIndexing(fieldType: String, value: TagValue): Option[TagValue] = {
    Try (fieldType match {
      case "string" => value.toString
      case "long" | "int" => value.toString.toLong
      case "bool" => value.toString.toBoolean
      case "double" => value.toString.toDouble
      case _ => value
    }) match {
      case Success(result) => Some(result)
      case Failure(_) =>
        // TODO: should we also log the tag name etc? wondering if input is crazy, then we might end up logging too many errors
        None
    }
  }

  /**
    * converts the tag into key value pair
    * @param tag span tag
    * @return TagValue(Any)
    */
  private def readTagValue(tag: Tag): TagValue = {
    import com.expedia.open.tracing.Tag.TagType._

    tag.getType match {
      case BOOL => tag.getVBool
      case STRING => tag.getVStr
      case LONG => tag.getVLong
      case DOUBLE => tag.getVDouble
      case BINARY => tag.getVBytes.toStringUtf8
      case _ => throw new RuntimeException(s"Fail to understand the span tag type ${tag.getType} !!!")
    }
  }
}
