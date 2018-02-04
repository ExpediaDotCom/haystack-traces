package com.expedia.www.haystack.trace.reader.readers

import com.expedia.open.tracing.api.Trace
import com.expedia.www.haystack.trace.reader.readers.transformers.{TraceTransformationHandler, TraceTransformer}
import com.expedia.www.haystack.trace.reader.readers.validators.{TraceValidationHandler, TraceValidator}

import scala.util.Try

class TraceProcessor(validators: Seq[TraceValidator],
                     preValidationTransformers: Seq[TraceTransformer],
                     postValidationTransformers: Seq[TraceTransformer]) {

  private val validationHandler: TraceValidationHandler = new TraceValidationHandler(validators)
  private val postTransformers: TraceTransformationHandler = new TraceTransformationHandler(postValidationTransformers)
  private val preTransformers: TraceTransformationHandler = new TraceTransformationHandler(preValidationTransformers)

  def process(trace: Trace): Try[Trace] = {
    for (trace <- Try(preTransformers.transform(trace));
         validated <- validationHandler.validate(trace)) yield postTransformers.transform(validated)
  }
}
