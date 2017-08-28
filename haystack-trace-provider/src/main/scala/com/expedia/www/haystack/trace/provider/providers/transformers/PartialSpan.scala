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

package com.expedia.www.haystack.trace.provider.providers.transformers

import com.expedia.open.tracing.{Log, Span, Tag}

import scala.collection.JavaConversions._

object PartialSpan {
  val SERVER_SEND_EVENT = "ss"
  val SERVER_RECV_EVENT = "sr"
  val CLIENT_SEND_EVENT = "cs"
  val CLIENT_RECV_EVENT = "cr"
}

class PartialSpan(first: Span, second: Span) {

  private def containsLogTag(span: Span, sendEvent: String, receiveEvent: String) = {
    span.getLogsList.contains((log: Log) => {
      log.getFieldsList.contains((tag: Tag) => {
        tag.getKey.equalsIgnoreCase("event") &&
          (tag.getVStr.equalsIgnoreCase(sendEvent) || tag.getVStr.equalsIgnoreCase(receiveEvent))
      })
    })
  }

  private def containsClientLogTag(span: Span) = containsLogTag(span, PartialSpan.CLIENT_RECV_EVENT, PartialSpan.CLIENT_RECV_EVENT)

  private def containsServerLogTag(span: Span) = containsLogTag(span, PartialSpan.SERVER_RECV_EVENT, PartialSpan.SERVER_SEND_EVENT)

  private def isFirstClient(): Boolean = {
    containsClientLogTag(first) ||
      containsServerLogTag(second) ||
      Seq[Span](first, second).min(Ordering.by((span: Span) => span.getStartTime)) == first
  }

  val clientSpan: Span = if (isFirstClient) first else second

  val serverSpan: Span = if (isFirstClient) second else first

  lazy val mergedSpan: Span = {
    Span
      .newBuilder(clientSpan)
      .addAllTags(serverSpan.getTagsList)
      .clearLogs().addAllLogs(clientSpan.getLogsList ++ serverSpan.getLogsList sortBy (_.getTimestamp))
      .build()
  }
}
