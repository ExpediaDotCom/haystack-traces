package com.expedia.www.haystack.stitch.span.metrics

import com.codahale.metrics.MetricRegistry

trait MetricsSupport {
  val metricRegistry: MetricRegistry = MetricsRegistries.metricRegistry
}

object MetricsRegistries {
  val metricRegistry = new MetricRegistry()
}
