package com.expedia.www.haystack.trace.reader.readers.validators

import com.expedia.open.tracing.api.Trace
import com.expedia.www.haystack.trace.reader.exceptions.InvalidTraceException

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class ParentIdValidator extends TraceValidator {
  override def validate(trace: Trace): Try[Trace] = {
    val spans = trace.getChildSpansList.toList
    val spanIdSet = spans.map(_.getSpanId).toSet

    if (!spans.forall(sp => spanIdSet.contains(sp.getParentSpanId) || sp.getParentSpanId.isEmpty))
      Failure(new InvalidTraceException(s"spans without valid parent found for traceId=${spans.head.getTraceId}"))
    else if (!spans.forall(sp => sp.getSpanId != sp.getParentSpanId))
      Failure(new InvalidTraceException(s"same parent and span id found for one ore more span for traceId=${spans.head.getTraceId}"))
    else
      Success(trace)
  }
}
