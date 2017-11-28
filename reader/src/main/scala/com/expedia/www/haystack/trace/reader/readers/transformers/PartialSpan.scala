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

import com.expedia.open.tracing.Span

import scala.collection.JavaConversions._

object PartialSpan {
  val SERVER_SEND_EVENT = "ss"
  val SERVER_RECV_EVENT = "sr"
  val CLIENT_SEND_EVENT = "cs"
  val CLIENT_RECV_EVENT = "cr"
}

class PartialSpan(first: Span, second: Span) {

  private def containsLogTag(span: Span, sendEvent: String, receiveEvent: String) = {
    span.getLogsList.exists(log => {
      log.getFieldsList.exists(tag => {
        tag.getKey.equalsIgnoreCase("event") &&
          (tag.getVStr.equalsIgnoreCase(sendEvent) || tag.getVStr.equalsIgnoreCase(receiveEvent))
      })
    })
  }

  private def containsClientLogTag(span: Span) = containsLogTag(span, PartialSpan.CLIENT_RECV_EVENT, PartialSpan.CLIENT_RECV_EVENT)

  private def containsServerLogTag(span: Span) = containsLogTag(span, PartialSpan.SERVER_RECV_EVENT, PartialSpan.SERVER_SEND_EVENT)

  val serverSpan: Span = if (containsServerLogTag(first)) first
  else if (containsServerLogTag(second)) second
  else if (containsClientLogTag(first)) second
  else if (containsClientLogTag(second)) first
  else Seq[Span](first, second).max(Ordering.by((span: Span) => span.getStartTime))

  val clientSpan: Span = if (serverSpan == second) first else second

  val mergedSpan: Span = {
    Span
      .newBuilder(serverSpan)
      .addAllTags(clientSpan.getTagsList)
      .clearLogs().addAllLogs(clientSpan.getLogsList ++ serverSpan.getLogsList sortBy (_.getTimestamp))
      .build()
  }
}
