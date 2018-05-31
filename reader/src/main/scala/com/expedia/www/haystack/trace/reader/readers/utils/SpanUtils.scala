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

import com.expedia.open.tracing.{Log, Span, Tag}

import scala.collection.JavaConverters._
import SpanMarkers._

object SpanUtils {
  def getEventTimestamp(span: Span, event: String): Long = {
    span.getLogsList.asScala.find(log => {
      log.getFieldsList.asScala.exists(tag => {
        tag.getKey.equalsIgnoreCase(LOG_EVENT_TAG_KEY) && tag.getVStr.equalsIgnoreCase(event)
      })
    }).get.getTimestamp
  }

  def isMergedSpan(span: Span): Boolean = {
    containsClientLogTag(span) && containsServerLogTag(span)
  }

  def containsServerLogTag(span: Span): Boolean = {
    containsLogTag(span, SERVER_RECV_EVENT) && containsLogTag(span, SERVER_SEND_EVENT)
  }

  def containsClientLogTag(span: Span): Boolean = {
    containsLogTag(span, CLIENT_RECV_EVENT) && containsLogTag(span, CLIENT_RECV_EVENT)
  }

  def addServerLogTag(span: Span): Span = {
    val receiveEventLog = Log.newBuilder()
      .setTimestamp(span.getStartTime)
      .addFields(
        Tag.newBuilder().setKey(LOG_EVENT_TAG_KEY).setVStr(SERVER_RECV_EVENT))

    val sendEventLog = Log.newBuilder()
      .setTimestamp(span.getStartTime + span.getDuration)
      .addFields(
        Tag.newBuilder().setKey(LOG_EVENT_TAG_KEY).setVStr(SERVER_SEND_EVENT) )

    span
      .toBuilder
      .addLogs(receiveEventLog)
      .addLogs(sendEventLog)
      .build()
  }

  def addClientLogTag(span: Span): Span = {
    val sendEventLog = Log.newBuilder()
      .setTimestamp(span.getStartTime)
      .addFields(
        Tag.newBuilder().setType(Tag.TagType.STRING).setKey(LOG_EVENT_TAG_KEY).setVStr(CLIENT_SEND_EVENT))

    val receiveEventLog = Log.newBuilder()
      .setTimestamp(span.getStartTime + span.getDuration)
      .addFields(
        Tag.newBuilder().setType(Tag.TagType.STRING).setKey(LOG_EVENT_TAG_KEY).setVStr(CLIENT_RECV_EVENT))

    span
      .toBuilder
      .addLogs(sendEventLog)
      .addLogs(receiveEventLog)
      .build()
  }

  private def containsLogTag(span: Span, event: String) = {
    span.getLogsList.asScala.exists(log => {
      log.getFieldsList.asScala.exists(tag => {
        tag.getKey.equalsIgnoreCase(LOG_EVENT_TAG_KEY) && tag.getVStr.equalsIgnoreCase(event)
      })
    })
  }
}

object SpanMarkers {
  val LOG_EVENT_TAG_KEY = "event"
  val SERVER_SEND_EVENT = "ss"
  val SERVER_RECV_EVENT = "sr"
  val CLIENT_SEND_EVENT = "cs"
  val CLIENT_RECV_EVENT = "cr"

  val SPAN_KIND_TAG_KEY = "span.kind"
  val SERVER_SPAN_KIND = "server"
  val CLIENT_SPAN_KIND = "client"
}
