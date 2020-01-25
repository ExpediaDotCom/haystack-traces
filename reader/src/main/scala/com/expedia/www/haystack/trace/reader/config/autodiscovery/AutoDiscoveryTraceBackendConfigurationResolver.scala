package com.expedia.www.haystack.trace.reader.config.autodiscovery

import com.expedia.www.haystack.trace.commons.config.entities.{GrpcClientConfig, TraceStoreBackends}
import com.expedia.www.haystack.trace.commons.config.reload.Reloadable
import com.expedia.www.haystack.trace.reader.config.entities.TraceBackendConfigurationResolver
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

case class AutoDiscoveryTraceBackendConfigurationResolver() extends TraceBackendConfigurationResolver with Reloadable {
  private val LOGGER = LoggerFactory.getLogger(classOf[AutoDiscoveryTraceBackendConfigurationResolver])

  override def get: TraceStoreBackends = traceBackends

  var traceBackends: TraceStoreBackends = TraceStoreBackends(Seq())

  override def name: String = "traceBackends"

  @volatile
  private var currentVersion: Int = 0

  private def hasConfigChanged(newConfigData: String): Boolean = newConfigData.hashCode != currentVersion

  override def onReload(newConfig: String): Unit = {
    if(StringUtils.isNotEmpty(newConfig) && hasConfigChanged(newConfig)) {
      LOGGER.info("New Storage Backends have been detected: " + newConfig)
      var elbDnsNames = newConfig.split(",")
      val port = 8090

      traceBackends = TraceStoreBackends(elbDnsNames.map( elb => GrpcClientConfig(elb, port)))

      currentVersion =  newConfig.hashCode
    }
  }
}
