package com.expedia.www.haystack.trace.reader.readers.transformers

import com.expedia.open.tracing.Span

import scala.collection.mutable

/**
  * dedup the spans with the same span id
  */
class DeDuplicateSpanTransformer extends TraceTransformer {

  override def transform(spans: List[Span]): List[Span] = {
    val seen = mutable.HashSet[Span]()
    spans.filter {
      span =>
        val alreadySeen = seen.contains(span)
        seen.add(span)
        !alreadySeen
    }
  }
}
