/*
 *  Copyright 2017 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.expedia.www.haystack.trace.reader.readers.transformers

import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.trace.reader.readers.utils.{AuxiliaryTags, PartialSpanMarkers, PartialSpanUtils}
import com.expedia.www.haystack.trace.reader.readers.utils.TagBuilders.{buildBoolTag, buildLongTag, buildStringTag}
import com.expedia.www.haystack.trace.reader.readers.utils.TagExtractors.extractTagStringValue

import scala.collection.JavaConverters._

/**
  * Merges partial spans and generates a single [[Span]] combining a client and corresponding server span
  */
class PartialSpanTransformer extends TraceTransformer {
  override def transform(spans: Seq[Span]): Seq[Span] = {
    spans.groupBy(span => span.getSpanId).map((pair) => pair._2 match {
      case Seq(span: Span) => span
      case list: Seq[Span] => mergeSpans(list)
    }).toSeq
  }

  def mergeSpans(spans: Seq[Span]): Span = {
    val serverOptional = collapseSpans(spans.filter(PartialSpanUtils.containsServerLogTag))
    val clientOptional = collapseSpans(spans.filter(PartialSpanUtils.containsClientLogTag))

    if(clientOptional.isDefined && serverOptional.isEmpty)
      clientOptional.get
    else if(serverOptional.isDefined && serverOptional.isEmpty)
      serverOptional.get
    else {
      val serverSpan = serverOptional.get
      val clientSpan = clientOptional.get
      Span
        .newBuilder(serverSpan)
        .addAllTags((clientSpan.getTagsList.asScala ++ auxiliaryCommonTags(clientSpan, serverSpan) ++ auxiliaryClientTags(clientSpan) ++ auxiliaryServerTags(serverSpan)).asJavaCollection)
        .clearLogs().addAllLogs((clientSpan.getLogsList.asScala ++ serverSpan.getLogsList.asScala.sortBy(_.getTimestamp)).asJavaCollection)
        .build()
    }
  }

  def collapseSpans(spans: Seq[Span]): Option[Span] = {
    spans match {
      case Nil => None
      case List(span) => Some(span)
      case _ =>
        val firstSpan = spans.minBy(_.getStartTime)
        val lastSpan = spans.maxBy(span => span.getStartTime + span.getDuration)
        val allTags = spans.flatMap(span => span.getTagsList.asScala)
        val allLogs = spans.flatMap(span => span.getLogsList.asScala)
        val opName = spans.map(_.getOperationName).reduce((a, b) => a + " & " + b)

        Some(
          Span
            .newBuilder(firstSpan)
            .setOperationName(opName)
            .setDuration(lastSpan.getStartTime + lastSpan.getDuration - firstSpan.getStartTime)
            .clearTags().addAllTags(allTags.asJava).addTags(buildBoolTag(AuxiliaryTags.ERR_IS_MULTI_PARTIAL_SPAN, tagValue = true))
            .clearLogs().addAllLogs(allLogs.asJava)
            .build())
    }
  }

  // Network delta - difference between server and client duration
  // calculate only if serverDuration is smaller then client
  private def calculateNetworkDelta(clientSpan: Span, serverSpan: Span): Option[Long] = {
    val clientDuration = PartialSpanUtils.getEventTimestamp(clientSpan, PartialSpanMarkers.CLIENT_RECV_EVENT) - PartialSpanUtils.getEventTimestamp(clientSpan, PartialSpanMarkers.CLIENT_SEND_EVENT)
    val serverDuration = PartialSpanUtils.getEventTimestamp(serverSpan, PartialSpanMarkers.SERVER_SEND_EVENT) - PartialSpanUtils.getEventTimestamp(serverSpan, PartialSpanMarkers.SERVER_RECV_EVENT)

    if (serverDuration < clientDuration) {
      Some(clientDuration - serverDuration)
    } else {
      None
    }
  }

  private def auxiliaryCommonTags(clientSpan: Span, serverSpan: Span): List[Tag]  =
    List(
      buildBoolTag(AuxiliaryTags.IS_MERGED_SPAN, tagValue = true),
      buildLongTag(AuxiliaryTags.NETWORK_DELTA, calculateNetworkDelta(clientSpan, serverSpan).getOrElse(-1))
    )

  private def auxiliaryClientTags(span: Span): List[Tag] =
    List(
      buildStringTag(AuxiliaryTags.CLIENT_SERVICE_NAME, span.getServiceName),
      buildStringTag(AuxiliaryTags.CLIENT_OPERATION_NAME, span.getOperationName),
      buildStringTag(AuxiliaryTags.CLIENT_INFRASTRUCTURE_PROVIDER, extractTagStringValue(span, AuxiliaryTags.INFRASTRUCTURE_PROVIDER)),
      buildStringTag(AuxiliaryTags.CLIENT_INFRASTRUCTURE_LOCATION, extractTagStringValue(span, AuxiliaryTags.INFRASTRUCTURE_LOCATION)),
      buildLongTag(AuxiliaryTags.CLIENT_START_TIME, span.getStartTime),
      buildLongTag(AuxiliaryTags.CLIENT_DURATION, span.getDuration)
    )

  private def auxiliaryServerTags(span: Span): List[Tag] = {
    List(
      buildStringTag(AuxiliaryTags.SERVER_SERVICE_NAME, span.getServiceName),
      buildStringTag(AuxiliaryTags.SERVER_OPERATION_NAME, span.getOperationName),
      buildStringTag(AuxiliaryTags.SERVER_INFRASTRUCTURE_PROVIDER, extractTagStringValue(span, AuxiliaryTags.INFRASTRUCTURE_PROVIDER)),
      buildStringTag(AuxiliaryTags.SERVER_INFRASTRUCTURE_LOCATION, extractTagStringValue(span, AuxiliaryTags.INFRASTRUCTURE_LOCATION)),
      buildLongTag(AuxiliaryTags.SERVER_START_TIME, span.getStartTime),
      buildLongTag(AuxiliaryTags.SERVER_DURATION, span.getDuration)
    )
  }
}
