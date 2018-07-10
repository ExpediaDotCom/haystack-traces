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
import com.expedia.www.haystack.trace.reader.readers.utils.{SpanTree, SpanUtils}

import scala.collection.Seq

/**
  * Fixes clock skew between parent and child spans
  * If any child spans reports a startTime earlier then parent span's startTime,
  * corresponding delta will be added in the subtree with child span as root
  * addSkewInSubtree looks into each child of given subtreeRoot, calculates delta,
  * and recursively applies delta in its subtree
  */
class ClockSkewFromParentTransformer extends TraceTransformer {

  override def transform(spans: Seq[Span]): Seq[Span] = {
    adjustSkew(SpanTree(spans.toList))
  }

  private def adjustSkew(node: SpanTree): Seq[Span] = {
    val children = node.children.map(child => adjustSpan(child.span, node.span))
    val childrensChildren = node.children.flatMap(child => child.children.flatMap(child => adjustSkew(child)))

    List(node.span) ++ children ++ childrensChildren
  }

  private def adjustSpan(child: Span, parent: Span): Span = {
    var shift = 0L
    if (child.getStartTime < parent.getStartTime) {
      shift = parent.getStartTime - child.getStartTime
    }
    val childEndTime = SpanUtils.getEndTime(child)
    val parentEndTime = SpanUtils.getEndTime(parent)
    if (parentEndTime < childEndTime + shift) {
      shift = parentEndTime - childEndTime
    }
    if (shift == 0L) {
      child
    } else {
      Span.newBuilder(child).setStartTime(child.getStartTime + shift).build()
    }
  }
}
