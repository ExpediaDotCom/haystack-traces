package com.expedia.www.haystack.trace.reader.config.autodiscovery

import com.expedia.www.haystack.commons.config.ConfigurationLoader
import com.expedia.www.haystack.trace.commons.config.reload.Reloadable
import com.expedia.www.haystack.trace.reader.config.entities.{ElasticSearchClientConfiguration, ElasticSearchClientConfigurationResolver}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import com.typesafe.config.Config

import scala.collection.mutable.ListBuffer

case class AutoDiscoveryElasticSearchClientConfigurationResolver() extends ElasticSearchClientConfigurationResolver with Reloadable {
  private val LOGGER = LoggerFactory.getLogger(classOf[AutoDiscoveryElasticSearchClientConfigurationResolver])
  private val config: Config = ConfigurationLoader.loadConfigFileWithEnvOverrides()

  override def get: Seq[ElasticSearchClientConfiguration] = elasticSearchClientConfig

  var elasticSearchClientConfig = new ListBuffer[ElasticSearchClientConfiguration]()

  override def name: String = "esClients"

  @volatile
  private var currentVersion: Int = 0

  private def hasConfigChanged(newConfigData: String): Boolean = newConfigData.hashCode != currentVersion

  override def onReload(newConfig: String): Unit = {
    if(StringUtils.isNotEmpty(newConfig) && hasConfigChanged(newConfig)) {
      LOGGER.info("New Storage Backends have been detected: " + newConfig)
      var esEndpoints = newConfig.split(",")

      val clientStaticConfig = config.getConfig("elasticsearch.client")

      val username = if (clientStaticConfig.hasPath("username")) {
        Option(clientStaticConfig.getString("username"))
      } else None
      val password = if (clientStaticConfig.hasPath("password")) {
        Option(clientStaticConfig.getString("password"))
      } else None

      esEndpoints.foreach( esEndpoint => elasticSearchClientConfig += ElasticSearchClientConfiguration(
        endpoint = "http://%s:80".format(esEndpoint),
        username = username,
        password = password,
        connectionTimeoutMillis = clientStaticConfig.getInt("conn.timeout.ms"),
        readTimeoutMillis = clientStaticConfig.getInt("read.timeout.ms")
      ))

      currentVersion =  newConfig.hashCode
    }
  }
}
