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
import com.expedia.open.tracing.api.Trace
import com.expedia.www.haystack.trace.reader.exceptions.InvalidTraceException

import scala.collection.JavaConversions._
import scala.util.Try

trait TraceValidationHandler {

  private def hasNonEmptyTraceId(traceId: String): Unit = {
    if (traceId.isEmpty) throw new InvalidTraceException("invalid traceId")
  }

  private def allSpansHaveAValidParent(spans: List[Span]): Unit = {
    val spanIdSet = spans.foldLeft(Set[String]())((set, span) => set + span.getSpanId)
    if (!spans.forall(sp => spanIdSet.contains(sp.getParentSpanId) || sp.getParentSpanId.isEmpty)) {
      throw new InvalidTraceException("spans without parent found")
    }
  }

  private def noSpanHasSameParentIdAndSpanId(spans: List[Span]): Unit = {
    if (!spans.forall(sp => sp.getSpanId != sp.getParentSpanId)) {
      throw new InvalidTraceException("same parent and span id found for a span")
    }
  }

  private def onlyOneSpanIsRoot(spans: List[Span]): Unit = {
    val rootCount = spans.count(_.getParentSpanId.isEmpty)
    if (rootCount != 1) throw new InvalidTraceException(s"found $rootCount roots")
  }

  private def allSpansHaveSameTraceId(spans: List[Span], traceId: String) = {
    if (!spans.forall(sp => sp.getTraceId.equals(traceId))) {
      throw new InvalidTraceException("span with different traceId are not allowed")
    }
  }

  def validate(trace: Trace): Try[Unit] = {
    val spans = trace.getChildSpansList.toList

    Try {
      hasNonEmptyTraceId(trace.getTraceId)

      allSpansHaveSameTraceId(spans, trace.getTraceId)

      noSpanHasSameParentIdAndSpanId(spans)

      onlyOneSpanIsRoot(spans)

      allSpansHaveAValidParent(spans)
    }
  }
}
