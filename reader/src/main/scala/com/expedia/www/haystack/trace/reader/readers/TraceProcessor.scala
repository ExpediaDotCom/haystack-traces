package com.expedia.www.haystack.trace.reader.readers

import com.expedia.open.tracing.api.Trace
import com.expedia.www.haystack.trace.reader.readers.transformers.{TraceTransformationHandler, TraceTransformer}
import com.expedia.www.haystack.trace.reader.readers.validators.{TraceValidationHandler, TraceValidator}

import scala.util.{Failure, Success, Try}

class TraceProcessor(validators: Seq[TraceValidator], transformers: Seq[TraceTransformer]) {

  private val validationHandler: TraceValidationHandler = new TraceValidationHandler(validators)
  private val transformationHandler: TraceTransformationHandler = new TraceTransformationHandler(transformers)

  def process(trace: Trace): Try[Trace] = {
    validationHandler.validate(trace) match {
      case Success(_) => Success(transformationHandler.transform(trace))
      case Failure(ex) => Failure(ex)
    }
  }
}
