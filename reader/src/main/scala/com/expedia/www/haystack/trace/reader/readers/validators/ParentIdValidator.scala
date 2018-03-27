package com.expedia.www.haystack.trace.reader.readers.validators

import com.expedia.open.tracing.api.Trace
import com.expedia.www.haystack.trace.reader.exceptions.InvalidTraceException

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * validates if spans in the trace has a valid parentIds
  * assumes that traceId is a non-empty string and there is a single root, apply [[TraceIdValidator]] and [[RootValidator]] to make sure
  */
class ParentIdValidator extends TraceValidator {
  override def validate(trace: Trace): Try[Trace] = {
    val spans = trace.getChildSpansList.asScala
    val spanIdSet = spans.map(_.getSpanId).toSet

    if (!spans.forall(sp => spanIdSet.contains(sp.getParentSpanId) || sp.getParentSpanId.isEmpty)) {
      Failure(new InvalidTraceException(s"spans without valid parent found for traceId=${spans.head.getTraceId}"))
    }  else if (!spans.forall(sp => sp.getSpanId != sp.getParentSpanId)) {
      Failure(new InvalidTraceException(s"same parent and span id found for one ore more span for traceId=${spans.head.getTraceId}"))
    }
    else {
      Success(trace)
    }
  }
}
