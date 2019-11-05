package com.expedia.www.haystack.trace.reader.config.entities
import java.util.concurrent.ConcurrentHashMap

import com.expedia.www.haystack.trace.commons.config.entities.TraceStoreBackends
import com.expedia.www.haystack.trace.commons.config.reload.Reloadable
import scala.collection.JavaConverters._

case class AwsDiscoveryTraceStoreBackedResolver extends TraceStoreBackendResolver with Reloadable {
  override val traceStoreBackends: TraceStoreBackends = _

  private val traceStoreBackends = new ConcurrentHashMap[String, TraceStoreBackends]()

  // fail fast
  override def name: String = ???

  /**
    * this is called whenever the configuration reloader system reads the configuration object from external store
    * we check if the config data has changed using the string's hashCode
    * @param configData config object that is loaded at regular intervals from external store
    */
  override def onReload(configData: String): Unit = ???

  /**
    * @return the whitelist index fields
    */
  def traceStoreBackends: Seq[TraceStoreBackends] = traceStoreBackends.values().asScala.toList
}
