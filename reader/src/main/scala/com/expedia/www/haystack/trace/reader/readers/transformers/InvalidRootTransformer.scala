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

/**
  *
  * If there are multiple roots in the given trace, use the first root based on startTime to be root
  * mark other roots as children of the selected root
  * If there is no root, assume loopback span or first span in time order to be root
  *
  * **Apply this transformer only if you are not confident about clients sending in roots properly**
  */
class InvalidRootTransformer extends TraceTransformer {

  override def transform(spans: List[Span]): List[Span] = {
    val roots = spans.filter(span => span.getParentSpanId.isEmpty)

    roots.size match {
      case 1 => spans
      case 0 => toTraceWithAssumedRoot(spans)
      case _ => toTraceWithSingleRoot(spans, roots)
    }
  }

  private def toTraceWithAssumedRoot(spans: List[Span]) = {
    val sortedSpans = spans.sortBy(_.getStartTime)

    // assume either loopback or first span as root
    val assumedRoot = sortedSpans
      .find(span => span.getParentSpanId == span.getSpanId)
      .getOrElse(sortedSpans.head)

    spans.map(span => if (span == assumedRoot) Span.newBuilder(span).setParentSpanId("").build() else span)
  }

  private def toTraceWithSingleRoot(spans: List[Span], roots: List[Span]) = {
    val earliestRoot = roots.minBy(_.getStartTime)

    spans.map(span =>
      if (span.getParentSpanId.isEmpty && span != earliestRoot) {
        Span.newBuilder(span).setParentSpanId(earliestRoot.getSpanId).build()
      } else {
        span
      }
    )
  }
}
