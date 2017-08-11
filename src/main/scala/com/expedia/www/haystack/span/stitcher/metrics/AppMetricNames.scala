package com.expedia.www.haystack.span.stitcher.metrics

/**
  * list all app metric names that are published on jmx
  */
object AppMetricNames {
  val STITCH_PUNCTUATE_TIMER = "stitch.punctuate"
  val STITCH_PROCESS_TIMER = "stitch.process"
  val STITCH_SPAN_COUNT = "stitch.span.count"
  val STATE_STORE_EVICTION = "state.store.eviction"
  val CHANGELOG_SEND_FAILURE = "changelog.send.failure"
  val SPAN_DESER_FAILURE = "span.deser.failure"
  val STITCHED_SPAN_DESER_FAILURE = "stitch.span.deser.failure"
}
