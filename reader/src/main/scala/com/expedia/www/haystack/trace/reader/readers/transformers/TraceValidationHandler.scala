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

  private def allSpansWithValidParent(spans: List[Span]): Unit = {
    val spanIdSet = spans.map(_.getSpanId).toSet
    if (!spans.forall(sp => spanIdSet.contains(sp.getParentSpanId) || sp.getParentSpanId.isEmpty)) {
      throw new InvalidTraceException(s"spans without valid parent found for traceId=${spans.head.getTraceId}")
    }
  }

  private def noSpanHasSameParentIdAndSpanId(spans: List[Span]): Unit = {
    if (!spans.forall(sp => sp.getSpanId != sp.getParentSpanId)) {
      throw new InvalidTraceException(s"same parent and span id found for one ore more span for traceId=${spans.head.getTraceId}")
    }
  }

  private def onlyOneSpanIsRoot(spans: List[Span]): Unit = {
    val roots = spans.filter(_.getParentSpanId.isEmpty).map(_.getSpanId).toSet
    if (roots.size != 1) throw new InvalidTraceException(s"found ${roots.size} roots with spanIDs=${roots.mkString(",")} " +
      s"and traceID=${spans.head.getTraceId}")
  }

  private def allSpansHaveSameTraceId(spans: List[Span], traceId: String) = {
    if (!spans.forall(sp => sp.getTraceId.equals(traceId))) {
      throw new InvalidTraceException(s"span with different traceId are not allowed for traceId=$traceId")
    }
  }

  def validate(trace: Trace): Try[Unit] = {
    val spans = trace.getChildSpansList.toList

    Try {
      hasNonEmptyTraceId(trace.getTraceId)

      allSpansHaveSameTraceId(spans, trace.getTraceId)

      noSpanHasSameParentIdAndSpanId(spans)

      onlyOneSpanIsRoot(spans)

      allSpansWithValidParent(spans)
    }
  }
}
