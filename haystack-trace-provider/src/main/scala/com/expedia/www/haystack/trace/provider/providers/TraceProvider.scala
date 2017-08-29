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

package com.expedia.www.haystack.trace.provider.providers

import com.expedia.open.tracing.Span
import com.expedia.open.tracing.internal._
import com.expedia.www.haystack.trace.provider.exceptions.SpanNotFoundException
import com.expedia.www.haystack.trace.provider.metrics.MetricsSupport
import com.expedia.www.haystack.trace.provider.providers.transformer.{ClockSkewTransformer, PartialSpanTransformer, TraceTransformationHandler, TraceValidationHandler}
import com.expedia.www.haystack.trace.provider.providers.transformers.SortSpanTransformer
import com.expedia.www.haystack.trace.provider.stores.TraceStore
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

class TraceProvider(traceStore: TraceStore)(implicit val executor: ExecutionContextExecutor)
  extends TraceTransformationHandler(Seq(new PartialSpanTransformer(), new ClockSkewTransformer(), new SortSpanTransformer()))
    with TraceValidationHandler
    with MetricsSupport {
  private val LOGGER: Logger = LoggerFactory.getLogger(s"${classOf[TraceProvider]}.search.trace.rejection")
  private val traceRejectedCounter = metricRegistry.meter("search.trace.rejection")

  private def transformTrace(trace: Trace): Try[Trace] = {
    validate(trace) match {
      case Success(_) => Success(transform(trace))
      case Failure(ex) => Failure(ex)
    }
  }

  private def transformTraceIgnoringInvalidSpans(trace: Trace): Option[Trace] = {
    validate(trace) match {
      case Success(_) => Some(transform(trace))
      case Failure(_) => {
        LOGGER.warn(s"invalid trace rejected $trace")
        traceRejectedCounter.mark()
        None
      }
    }
  }

  def getTrace(request: TraceRequest): Future[Trace] = {
    traceStore
      .getTrace(request.getTraceId)
      .map(transformTrace(_).get) // TODO handle try and return failure future
  }

  def getRawTrace(request: TraceRequest): Future[Trace] = {
    traceStore.getTrace(request.getTraceId)
  }

  def getRawSpan(request: SpanRequest): Future[Span] = {
    traceStore.getTrace(request.getTraceId).map(trace => {
      val spanOption = trace.getChildSpansList
        .find(span => span.getSpanId.equals(request.getSpanId))

      spanOption match {
        case Some(span) => span
        case None => throw new SpanNotFoundException // TODO failure future
      }
    })
  }

  def searchTraces(request: TracesSearchRequest): Future[TracesSearchResult] = {
    traceStore
      .searchTraces(request)
      .map(
        traces => {
          TracesSearchResult
            .newBuilder()
            .addAllTraces(traces.flatMap(transformTraceIgnoringInvalidSpans))
            .build()
        })
  }
}
