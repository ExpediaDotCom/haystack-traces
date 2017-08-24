package com.expedia.www.haystack.span.bufferer.metrics

/**
  * list all app metric names that are published on jmx
  */
object AppMetricNames {
  val PUNCTUATE_TIMER = "buffer.punctuate"
  val PROCESS_TIMER = "buffer.process"
  val BUFFERED_SPANS_COUNT = "buffered.spans.count"
  val STATE_STORE_EVICTION = "state.store.eviction"
  val CHANGELOG_SEND_FAILURE = "changelog.send.failure"
  val SPAN_PROTO_DESER_FAILURE = "span.proto.deser.failure"
  val SPAN_BUFFER_PROTO_DESER_FAILURE = "span.buffer.proto.deser.failure"
}
