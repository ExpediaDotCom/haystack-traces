package com.expedia.www.haystack.trace.reader.readers.validators

import com.expedia.open.tracing.api.Trace
import com.expedia.www.haystack.trace.reader.exceptions.InvalidTraceException

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class SingleRootValidator extends TraceValidator {
  override def validate(trace: Trace): Try[Trace] = {
    val roots = trace.getChildSpansList.toList.filter(_.getParentSpanId.isEmpty).map(_.getSpanId).toSet

    if (roots.size != 1)
      Failure(new InvalidTraceException(s"found ${roots.size} roots with spanIDs=${roots.mkString(",")} and traceID=${trace.getTraceId}"))
    else
      Success(trace)
  }
}
