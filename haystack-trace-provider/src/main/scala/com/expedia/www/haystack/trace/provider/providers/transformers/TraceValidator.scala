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
import com.expedia.open.tracing.internal.Trace
import com.expedia.www.haystack.trace.provider.exceptions.InvalidTraceException

import scala.collection.JavaConversions._

object TraceValidator {
  private def onlyOneSpanHasLoopback(spans: List[Span]): Unit = {
    val rootCount = spans.count(span => span.getParentSpanId == null || span.getParentSpanId.isEmpty)
    if (rootCount != 1)
      throw new InvalidTraceException(s"found $rootCount roots")
  }

  private def spansHaveAValidParent(spans: List[Span]): Unit = {
    val spanIdSet = spans.foldLeft(Set[String]())((set, span) =>
      if (span.getSpanId == null)
        set
      else
        set + span.getSpanId)
    if (spans.forall(span => span.getSpanId == null || spanIdSet.contains(span.getParentSpanId)))
      throw new InvalidTraceException("spans without parent found")
  }

  private def noSpanHasSameParentAndSpanIds(spans: List[Span]): Unit =
    if (spans.forall(span => !span.getSpanId.equals(span.getParentSpanId)))
      throw new InvalidTraceException("invalid traceId")

  def validate(trace: Trace) = {
    // non-empty traceId
    if (trace.getTraceId == null || trace.getTraceId.isEmpty)
      throw new InvalidTraceException("invalid traceId")

    // all spans must have the same traceId
    val spans = trace.getChildSpansList.toList
    if (!spans.forall(_.getTraceId.equals(trace.getTraceId)))
      throw new InvalidTraceException("span with same spanId and traceId are not allowed")

    noSpanHasSameParentAndSpanIds(spans)

    onlyOneSpanHasLoopback(spans)

    spansHaveAValidParent(spans)
  }
}
