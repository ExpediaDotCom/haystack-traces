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

import com.expedia.open.tracing.Span
import scala.collection.JavaConverters._

object PartialSpanUtils {
  def getEventTimestamp(span: Span, event: String): Long =
    span.getLogsList.asScala.find(log => {
      log.getFieldsList.asScala.exists(tag => {
        tag.getKey.equalsIgnoreCase("event") && tag.getVStr.equalsIgnoreCase(event)
      })
    }).get.getTimestamp

  def isMergedSpan(span: Span): Boolean = containsClientLogTag(span) && containsServerLogTag(span)

  def containsServerLogTag(span: Span): Boolean = {
    containsLogTag(span, PartialSpanMarkers.SERVER_RECV_EVENT) && containsLogTag(span, PartialSpanMarkers.SERVER_SEND_EVENT)
  }

  def containsClientLogTag(span: Span): Boolean = {
    containsLogTag(span, PartialSpanMarkers.CLIENT_RECV_EVENT) && containsLogTag(span, PartialSpanMarkers.CLIENT_RECV_EVENT)
  }

  private def containsLogTag(span: Span, event: String) = {
    span.getLogsList.asScala.exists(log => {
      log.getFieldsList.asScala.exists(tag => {
        tag.getKey.equalsIgnoreCase("event") && tag.getVStr.equalsIgnoreCase(event)
      })
    })
  }
}

object PartialSpanMarkers {
  val SERVER_SEND_EVENT = "ss"
  val SERVER_RECV_EVENT = "sr"
  val CLIENT_SEND_EVENT = "cs"
  val CLIENT_RECV_EVENT = "cr"
}
