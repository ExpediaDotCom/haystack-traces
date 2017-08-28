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

  private def addSkewInSubtree(subtreeRoot: Span, spans: List[Span], skew: Long): scala.List[Span] = {
    val children = spans.filter(_.getParentSpanId == subtreeRoot.getSpanId)

    val skewAdjustedRoot =
      if (skew > 0) Span.newBuilder(subtreeRoot).setStartTime(subtreeRoot.getStartTime + skew).build()
      else subtreeRoot

    skewAdjustedRoot ::
      children.flatMap(
        child => {
          val delta =
            if (subtreeRoot.getStartTime > child.getStartTime) subtreeRoot.getStartTime - child.getStartTime
            else 0
          addSkewInSubtree(child, spans, skew + delta)
        })
  }

  override def transform(spans: List[Span]): List[Span] = {
    val root = spans.find(_.getParentSpanId.isEmpty).get
    addSkewInSubtree(root, spans, 0)
  }
}
