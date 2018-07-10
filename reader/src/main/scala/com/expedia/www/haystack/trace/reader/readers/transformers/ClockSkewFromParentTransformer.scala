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
import com.expedia.www.haystack.trace.reader.readers.utils.{MutableSpanForest, SpanTree, SpanUtils}

import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}

/**
  * Fixes clock skew between parent and child spans
  * If any child spans reports a startTime earlier then parent span's startTime or
  * an endTime later then the parent span's endTime, then
  * the child span's startTime or endTime will be shifted. Where the shift is
  * set equal to the parent's startTime or endTime depending on which is off.
  */
class ClockSkewFromParentTransformer extends SpanTreeTransformer {

  override def transform(forest: MutableSpanForest): MutableSpanForest = {
    var underlyingSpans = new mutable.ListBuffer[Span]
    forest.getAllTrees.foreach(tree => {
      underlyingSpans += tree.span
      adjustSkew(tree.children, tree.span, underlyingSpans)
    })
    forest.updateUnderlyingSpans(underlyingSpans)
  }

  private def adjustSkew(childrenTrees: Seq[SpanTree], parent: Span, fixedSpans: ListBuffer[Span]): Unit = {
    fixedSpans ++ childrenTrees.map(child => adjustSpan(child.span, parent))
    childrenTrees.foreach(tree => adjustSkew(tree.children, tree.span, fixedSpans))
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
