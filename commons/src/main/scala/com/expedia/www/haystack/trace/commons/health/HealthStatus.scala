package com.expedia.www.haystack.trace.commons.health

object HealthStatus extends Enumeration {
  type HealthStatus = Value
  val HEALTHY, UNHEALTHY, NOT_SET = Value
}
