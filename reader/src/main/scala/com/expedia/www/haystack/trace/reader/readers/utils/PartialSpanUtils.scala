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

package com.expedia.www.haystack.trace.reader.readers.utils

import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.trace.reader.readers.utils.TagExtractors._
import com.expedia.www.haystack.trace.reader.readers.utils.TagBuilders.{buildStringTag, _}

import scala.collection.JavaConversions._

object PartialSpanUtils {
  // merge sever and client spans to a single merged span, only if corresponding event tags are present
  // use server span as primary while merging
  // return None otherwise
  def mergeSpans(first: Span, second: Span): Option[Span] = {
    val serverOptional = getServerSpan(first, second)

    serverOptional match {
      case Some(serverSpan) =>
        val clientSpan = if(serverSpan == first) second else first
        Some(Span
          .newBuilder(serverSpan)
          .addAllTags(clientSpan.getTagsList ++ auxiliaryCommonTags(clientSpan, serverSpan) ++ auxiliaryClientTags(clientSpan) ++ auxiliaryServerTags(serverSpan))
          .clearLogs().addAllLogs(clientSpan.getLogsList ++ serverSpan.getLogsList sortBy (_.getTimestamp))
          .build())
      case _ => None
    }
  }

  // merge multiple spans to single merged spans, use first span(based on startTime) as primary
  def mergeAllSpans(spans: List[Span]): Span = {
    val serverSpans = spans.filter(containsServerLogTag).sortBy(_.getStartTime)
    val clientSpans = spans.filter(containsClientLogTag).sortBy(_.getStartTime)
    val otherSpans = spans.filter(span => !containsClientLogTag(span) && !containsServerLogTag(span)).sortBy(_.getStartTime)

    List.concat(serverSpans, clientSpans, otherSpans).reduce((first, second) => {
      Span
        .newBuilder(first)
        .addAllTags(second.getTagsList)
        .clearLogs().addAllLogs(first.getLogsList ++ second.getLogsList sortBy (_.getTimestamp))
        .build()
    })
  }

  def getEventTimestamp(span: Span, event: String): Long =
    span.getLogsList.find(log => {
      log.getFieldsList.exists(tag => {
        tag.getKey.equalsIgnoreCase("event") && tag.getVStr.equalsIgnoreCase(event)
      })
    }).get.getTimestamp

  def isMergedSpan(span: Span): Boolean = containsClientLogTag(span) && containsServerLogTag(span)

  private def containsServerLogTag(span: Span) =
    containsLogTag(span, PartialSpanMarkers.SERVER_RECV_EVENT) && containsLogTag(span, PartialSpanMarkers.SERVER_SEND_EVENT)

  private def containsClientLogTag(span: Span) =
    containsLogTag(span, PartialSpanMarkers.CLIENT_RECV_EVENT) && containsLogTag(span, PartialSpanMarkers.CLIENT_RECV_EVENT)

  private def containsLogTag(span: Span, event: String) = {
    span.getLogsList.exists(log => {
      log.getFieldsList.exists(tag => {
        tag.getKey.equalsIgnoreCase("event") && tag.getVStr.equalsIgnoreCase(event)
      })
    })
  }

  private def getServerSpan(first: Span, second: Span): Option[Span] =
  if (containsServerLogTag(second) && containsClientLogTag(first)) {
    Some(second)
  } else {
    if (containsServerLogTag(first) && containsClientLogTag(second)) Some(first) else None
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
      buildLongTag(AuxiliaryTags.NETWORK_DELTA, calculateNetworkDelta(clientSpan, serverSpan).getOrElse(-1)),
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


object PartialSpanMarkers {
  val SERVER_SEND_EVENT = "ss"
  val SERVER_RECV_EVENT = "sr"
  val CLIENT_SEND_EVENT = "cs"
  val CLIENT_RECV_EVENT = "cr"
}
