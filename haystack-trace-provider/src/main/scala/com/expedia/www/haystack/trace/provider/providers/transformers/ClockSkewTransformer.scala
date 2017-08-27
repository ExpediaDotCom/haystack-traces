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

class ClockSkewTransformer extends TraceTransformer {
  private def filterDirectChildren(parentSpanId: String, descendants: List[Span]): List[Span] = {
    descendants
      .filter(span => span.getParentSpanId == parentSpanId || span.getSpanId == parentSpanId)
      .groupBy(_.getSpanId)
      .mapValues((partialSpans) => partialSpans.sortBy(_.getStartTime).head)
      .values.toList
  }

  private def addSkewRec(parent: Span, spans: List[Span], skew: Long): scala.List[Span] = {
    val descendants = spans.filterNot(parent == _)
    val children = filterDirectChildren(parent.getSpanId, descendants)

    val skewAdjustedParent = if (skew > 0)
      Span.newBuilder(parent).setStartTime(parent.getStartTime + skew).build()
    else
      parent

    skewAdjustedParent ::
      children.flatMap(
        child => {
          val delta = if (parent.getStartTime - child.getStartTime > 0) parent.getStartTime - child.getStartTime else 0
          addSkewRec(child, descendants, skew + delta)
        })
  }

  override def transform(spans: List[Span]): List[Span] = {
    val root = spans.find(_.getParentSpanId.isEmpty).get
    addSkewRec(root, spans, 0)
  }
}
