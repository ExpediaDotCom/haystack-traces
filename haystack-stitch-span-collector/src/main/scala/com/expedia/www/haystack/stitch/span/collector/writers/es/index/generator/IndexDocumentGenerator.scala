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

package com.expedia.www.haystack.stitch.span.collector.writers.es.index.generator

import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.stitch.span.collector.config.entities.IndexConfiguration
import com.expedia.www.haystack.stitch.span.collector.writers.es.index.generator.Document.{IndexDataModel, OperationName, TagKey, TagValue}
import org.apache.commons.lang3.StringUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class IndexDocumentGenerator(config: IndexConfiguration) {

  protected implicit val formats = DefaultFormats

  def create(traceId: String, spans: Seq[Span]): Option[Document] = {
    val result = transform(spans)
    if (result.nonEmpty) Some(Document(s"${traceId}_${spans.last.getSpanId}", Serialization.write(result))) else None
  }

  private def isValidSpanForIndex(sp: Span): Boolean = {
    sp.getProcess != null && StringUtils.isNotEmpty(sp.getProcess.getServiceName) && StringUtils.isNotEmpty(sp.getOperationName)
  }

  /**
    * transforms a span object into a map of key-value pairs for indexing
    * @param spans a span object
    * @return map of key-values
    */
  private def transform(spans: Seq[Span]): IndexDataModel = {
    val serviceNameMap = Document.newIndexDataModel

    // add service, operation names and duration as default indexing fields
    for (sp <- spans; if isValidSpanForIndex(sp)) {

      val operationMap = serviceNameMap.getOrElseUpdate(sp.getProcess.getServiceName, mutable.Map[OperationName, IndexingAttributes]())

      val indexData = operationMap.getOrElseUpdate(sp.getOperationName,
        IndexingAttributes(mutable.Map[TagKey, mutable.Set[TagValue]](), sp.getDuration, sp.getDuration))

      indexData.updateMinMaxDuration(sp.getDuration)

      // index the tags that are configured to be indexed.
      for (tag <- sp.getTagsList;
           indexField = config.keyedTags.get(tag.getKey); if indexField.isDefined && indexField.get.enabled;
           (k, v) = convertToKeyValue(tag);
           convertedToIndexingType = convertValueToIndexAttrType(indexField.get.`type`, v); if convertedToIndexingType.isDefined) {
        indexData.updateTags(k, convertedToIndexingType.get)
      }
    }
    serviceNameMap.values foreach { operationMap =>
      operationMap.put("_all", operationMap.values.foldRight(IndexingAttributes(mutable.Map(), Long.MaxValue, Long.MinValue)){
        (v, aggr) => aggr.merge(v)
      })
    }
    serviceNameMap
  }

  private def convertValueToIndexAttrType(attrType: String, value: TagValue): Option[TagValue] = {
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
