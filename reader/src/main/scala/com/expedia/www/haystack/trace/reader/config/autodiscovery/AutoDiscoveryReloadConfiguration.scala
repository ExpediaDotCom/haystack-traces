package com.expedia.www.haystack.trace.reader.config.autodiscovery

import com.expedia.www.haystack.trace.commons.config.reload.Reloadable

/**
 * defines the configuration parameters for reloading the app configs from external store like ElasticSearch
 *
 * @param reloadIntervalInMillis: app config will be refreshed after this given interval in millis
 * @param observers: list of reloadable configuration objects that subscribe to the reloader
 * @param loadOnStartup: loads the app configuration from external store on startup, default is true
 */
case class AutoDiscoveryReloadConfiguration(
                                           region: String,
                                           tags: Map[String,String],
                                           reloadIntervalInMillis: Int,
                                           observers: Seq[Reloadable],
                                           loadOnStartup: Boolean = true)
