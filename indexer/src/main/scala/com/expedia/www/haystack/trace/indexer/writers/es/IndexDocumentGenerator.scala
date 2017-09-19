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

import java.util.function.Supplier

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.trace.commons.clients.es.document.{Document, TraceIndexDoc}
import com.expedia.www.haystack.trace.commons.clients.es.document.Document.{TagKey, TagValue}
import com.expedia.www.haystack.trace.indexer.config.entities.{IndexConfiguration, IndexField}
import com.expedia.www.haystack.trace.indexer.metrics.MetricsSupport
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions._
import scala.collection.immutable.Stream
import scala.collection.mutable
import scala.util.{Failure, Random, Success, Try}

class IndexDocumentGenerator(config: IndexConfiguration) extends MetricsSupport {

  private val ELASTIC_SEARCH_DOC_ID_SUFFIX_LENGTH = 4
  private val randomCharStream = ThreadLocal.withInitial(new Supplier[Stream[Char]] {
    override def get(): Stream[Char] = Random.alphanumeric
  })

  /**
    * we append a random id of length 4 to every elasticSearch index document for a given span buffer
    * Since the span buffer can be in partial form and we may see another another span buffer object for
    * the same TraceId, therefore we assign a random id to the elastic search document to avoid conflicts.
    * With this, now every elastic search document bears the id : (TraceId)_(RANDOM_ID_OF_SIZE_4)
    * @param spanBuffer a span buffer object
    * @return index document that can be put in elastic search
    */

  def createIndexDocument(traceId: String, spanBuffer: SpanBuffer): Option[Document] = {
    // We maintain a white list of tags that are to be indexed. The whitelist is maintained as a confguration
    // in an external database (outside this app boundary). However, the app periodically reads this whitelist config
    // and applies it to the new spans that are read.
    val whitelistTagKeys = config.indexableTagsByTagName.get()

    val spanIndices = for(sp <- spanBuffer.getChildSpansList; if isValidForIndex(sp)) yield transform(sp, whitelistTagKeys)
    val docId = s"${traceId}_${randomCharStream.get().take(ELASTIC_SEARCH_DOC_ID_SUFFIX_LENGTH).mkString}"
    if (spanIndices.nonEmpty) Some(Document(docId, TraceIndexDoc(duration(spanBuffer), spanIndices))) else None
  }

  private def isValidForIndex(span: Span): Boolean = {
    StringUtils.isNotEmpty(span.getServiceName) && StringUtils.isNotEmpty(span.getOperationName)
  }

  // finds the amount of time it takes for one trace(span buffer) to complete.
  // span buffer contains all the spans for a given TraceId
  private def duration(spanBuffer: SpanBuffer): Long = {
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
  private def transform(span: Span, whitelistTagKeys: Map[String, IndexField]): mutable.Map[String, Any] = {
    val spanIndexDoc = mutable.Map[String, Any]()

    def addTagKeys(tags: java.util.List[Tag]): Unit = {
      for (tag <- tags;
           indexField = whitelistTagKeys.get(tag.getKey)
           if indexField.isDefined && indexField.get.enabled;
           (k, v) = transformTagToKVPair(tag);
           convertedToIndexFieldType = adjustTagValueToIndexFieldType(indexField.get.`type`, v)
           if convertedToIndexFieldType.isDefined) {
        val tagValues = spanIndexDoc.getOrElseUpdate(k, mutable.ListBuffer[TagValue]()).asInstanceOf[mutable.ListBuffer[TagValue]]
        tagValues += convertedToIndexFieldType.get
      }
    }

    addTagKeys(span.getTagsList)
    span.getLogsList.foreach(logEntry => addTagKeys(logEntry.getFieldsList))

    spanIndexDoc.put("spanid", span.getSpanId)
    spanIndexDoc.put("service", span.getServiceName)
    spanIndexDoc.put("operation", span.getOperationName)
    spanIndexDoc.put("duration", span.getDuration)

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
  private def adjustTagValueToIndexFieldType(fieldType: String, value: TagValue): Option[TagValue] = {
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
    * @return TagKey(string), TagValue(Any)
    */
  private def transformTagToKVPair(tag: Tag): (TagKey, TagValue) = {
    import com.expedia.open.tracing.Tag.TagType._

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
