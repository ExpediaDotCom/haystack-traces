package com.expedia.www.haystack.trace.indexer.health

import java.util.concurrent.atomic.AtomicReference

import com.expedia.www.haystack.trace.indexer.health.HealthStatus.HealthStatus
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * provides the health check of app
  */
object HealthController {

  private val LOGGER = LoggerFactory.getLogger(HealthController.getClass)

  // sets the initial health state as 'not set'
  private val status = new AtomicReference[HealthStatus](HealthStatus.NOT_SET)

  private var listeners = mutable.ListBuffer[HealthStatusChangeListener]()

  /**
    * set the app status as health
    */
  def setHealthy(): Unit = {
    LOGGER.info("Setting the app status as 'HEALTHY'")
    if(status.getAndSet(HealthStatus.HEALTHY) != HealthStatus.HEALTHY) notifyChange(HealthStatus.HEALTHY)
  }

  /**
    * set the app status as unhealthy
    */
  def setUnhealthy(): Unit = {
    LOGGER.error("Setting the app status as 'UNHEALTHY'")
    if(status.getAndSet(HealthStatus.UNHEALTHY) != HealthStatus.UNHEALTHY) notifyChange(HealthStatus.UNHEALTHY)
  }

  /**
    * @return true if app is healthy else false
    */
  def isHealthy: Boolean = status.get() == HealthStatus.HEALTHY

  /**
    * add health change listener that will be called on any change in the health status
    * @param l listener
    */
  def addListener(l: HealthStatusChangeListener): Unit = listeners += l

  private def notifyChange(status: HealthStatus): Unit = {
    listeners foreach {
      l =>
        l.onChange(status)
    }
  }
}
