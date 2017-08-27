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

package com.expedia.www.haystack.trace.provider.providers.transformer

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trace.provider.exceptions.InvalidTraceException

import scala.collection.JavaConversions._

class PartialSpanTransformer extends TraceTransformer {
  private def merge(clientSpan: Span, serverSpan: Span): Span = {
    Span
      .newBuilder(clientSpan)
      .addAllTags(serverSpan.getTagsList)
      .clearLogs().addAllLogs(clientSpan.getLogsList ++ serverSpan.getLogsList sortBy (_.getTimestamp))
      .build()
  }

  override def transform(spans: List[Span]): List[Span] = {
    spans.groupBy(_.getSpanId).map((pair) => pair._2 match {
      case List(span: Span) => span
      case List(firstSpan: Span, secondSpan: Span) => {
        val orderedSpans = Seq[Span](firstSpan, secondSpan).sortBy(_.getStartTime)
        merge(orderedSpans(0), orderedSpans(1))
      }
      case _ => throw new InvalidTraceException("more then 2 spans have same spanIds")
    }).toList
  }
}
