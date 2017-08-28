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
import com.expedia.www.haystack.trace.provider.providers.transformers.PartialSpan

class ClockSkewTransformer extends TraceTransformer {
  // extracting direct child of give span
  // in case of partial spans, pick the client span, server span is considered as child of client span
  private def filterDirectChildren(parentSpanId: String, descendants: List[Span]): List[Span] = {
    descendants
      .filter(span => span.getParentSpanId == parentSpanId || span.getSpanId == parentSpanId)
      .groupBy(_.getSpanId)
      .mapValues((partialSpans) =>
        if(partialSpans.length > 1) new PartialSpan(partialSpans(0), partialSpans(1)).clientSpan
        else partialSpans(0))
      .values.toList
  }

  private def addSkewInSubtree(root: Span, descendants: List[Span], skew: Long): scala.List[Span] = {
    val children = filterDirectChildren(root.getSpanId, descendants)

    val skewAdjustedRoot =
      if (skew > 0) Span.newBuilder(root).setStartTime(root.getStartTime + skew).build()
      else root

    skewAdjustedRoot ::
      children.flatMap(
        child => {
          val delta =
            if (root.getStartTime > child.getStartTime) root.getStartTime - child.getStartTime
            else 0
          addSkewInSubtree(child, descendants.filterNot(child == _), skew + delta)
        })
  }

  override def transform(spans: List[Span]): List[Span] = {
    val root = spans.find(_.getParentSpanId.isEmpty).get
    addSkewInSubtree(root, spans.filterNot(root == _), 0)
  }
}
