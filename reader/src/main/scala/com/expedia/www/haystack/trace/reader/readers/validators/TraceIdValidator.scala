package com.expedia.www.haystack.trace.reader.readers.validators

import com.expedia.open.tracing.api.Trace
import com.expedia.www.haystack.trace.reader.exceptions.InvalidTraceException

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * validates if the traceId is non empty and consistent for all the spans
  */
class TraceIdValidator extends TraceValidator {
  override def validate(trace: Trace): Try[Trace] =
    if (trace.getTraceId.isEmpty) {
      Failure(new InvalidTraceException("invalid traceId"))
    } else if (!trace.getChildSpansList.toList.forall(_.getTraceId == trace.getTraceId)) {
      Failure(new InvalidTraceException(s"span with different traceId are not allowed for traceId=${trace.getTraceId}"))
    } else {
      Success(trace)
    }
}
