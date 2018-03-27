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
import com.expedia.www.haystack.trace.reader.readers.utils.{PartialSpanMarkers, PartialSpanUtils}

/**
  * Fixes clock skew between parent and child spans
  * If any child spans reports a startTime earlier then parent span's startTime,
  * corresponding delta will be added in the subtree with child span as root
  *
  * addSkewInSubtree looks into each child of given subtreeRoot, calculates delta,
  * and recursively applies delta in its subtree
  */
class ClockSkewTransformer extends TraceTransformer {

  override def transform(spans: Seq[Span]): Seq[Span] = {
    adjustSkew(SpanTree(spans.toList), None)
  }

  private def adjustSkew(node: SpanTree, previousSkew: Option[Skew]): Seq[Span] = {
    val previousSkewAdjustedSpan: Span = previousSkew match {
      case Some(skew) => adjustForASpan(node.span, skew)
      case None => node.span
    }

    getClockSkew(previousSkewAdjustedSpan) match {
      case Some(skew) =>
        val selfSkewAdjustedSpan: Span = adjustForASpan(previousSkewAdjustedSpan, skew)
        selfSkewAdjustedSpan :: node.children.flatMap(adjustSkew(_, Some(skew)))
      case None =>
        previousSkewAdjustedSpan :: node.children.flatMap(adjustSkew(_, None))
    }
  }

  private def adjustForASpan(span: Span, skew: Skew): Span = {
    if (span.getServiceName == skew.serviceName) {
      Span
        .newBuilder(span)
        .setStartTime(span.getStartTime - skew.delta)
        .build()
    }
    else {
      span
    }
  }

  // if span is a merged span of partial spans, calculate corresponding skew
  private def getClockSkew(span: Span): Option[Skew] = {
    if (PartialSpanUtils.isMergedSpan(span)) {
      calculateClockSkew(
        PartialSpanUtils.getEventTimestamp(span, PartialSpanMarkers.CLIENT_SEND_EVENT),
        PartialSpanUtils.getEventTimestamp(span, PartialSpanMarkers.CLIENT_RECV_EVENT),
        PartialSpanUtils.getEventTimestamp(span, PartialSpanMarkers.SERVER_RECV_EVENT),
        PartialSpanUtils.getEventTimestamp(span, PartialSpanMarkers.SERVER_SEND_EVENT),
        span.getServiceName
      )
    } else {
      None
    }
  }

  /**
    * Calculate the clock skew between two servers based on logs in a span
    *
    * Only adjust for clock skew if logs are not in the following order:
    * Client send -> Server receive -> Server send -> Client receive
    *
    * Special case: if the server (child) span is longer than the client (parent), then do not
    * adjust for clock skew.
    */
  private def calculateClockSkew(
                            clientSend: Long,
                            clientRecv: Long,
                            serverRecv: Long,
                            serverSend: Long,
                            serviceName: String
                          ): Option[Skew] = {
    val clientDuration = clientRecv - clientSend
    val serverDuration = serverSend - serverRecv

    // There is only clock skew if CS is after SR or CR is before SS
    val csAhead = clientSend < serverRecv
    val crAhead = clientRecv > serverSend
    if (serverDuration > clientDuration || (csAhead && crAhead)) {
      None
    } else {
      val latency = (clientDuration - serverDuration) / 2
      serverRecv - latency - clientSend match {
        case 0 => None
        case _ => Some(Skew(serviceName, serverRecv - latency - clientSend))
      }
    }
  }

  case class Skew(serviceName: String, delta: Long)

  object SpanTree {
    def apply(spans: List[Span]): SpanTree = {
      build(spans.find(_.getParentSpanId.isEmpty).get, spans)
    }

    private def build(root: Span, spans: List[Span]): SpanTree = {
      val childTrees = spans.
        filter(span => span.getParentSpanId == root.getSpanId)
        .map(build(_, spans))

      SpanTree(root, childTrees)
    }
  }

  case class SpanTree(span: Span, children: List[SpanTree])
}
