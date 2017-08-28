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

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class TraceTransformationHandler(transformers: Seq[TraceTransformer]) {
  private val transformerChain = Function.chain(
    transformers
      .foldLeft(Seq[List[Span] => List[Span]]())((seq, t) => seq :+ t.transform _))

  def transform(trace: Trace): Try[Trace] = {
    TraceValidator.validate(trace) match {
      case Success(_) =>
        val transformedSpans = transformerChain.apply(trace.getChildSpansList.toList)
        Success(Trace
          .newBuilder()
          .setTraceId(trace.getTraceId)
          .addAllChildSpans(transformedSpans)
          .build())

      case Failure(ex) => Failure(ex)
    }
  }
}
