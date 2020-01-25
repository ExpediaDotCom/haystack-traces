package com.expedia.www.haystack.trace.reader.config.autodiscovery

import com.expedia.www.haystack.commons.retries.RetryOperation
import com.expedia.www.haystack.trace.commons.config.reload.ConfigurationReloadProvider

import scala.util.{Failure, Success}

class ConfigurationReloadElasticsearchClientsProvider(reloadConfig: AutoDiscoveryReloadConfiguration)
  extends ConfigurationReloadProvider(reloadConfig.reloadIntervalInMillis) {

  /**
   * loads the configuration from external store
   */
  override def load(): Unit = {
    reloadConfig.observers.foreach(observer => {

      RetryOperation.executeWithRetryBackoff(() => AwsElasticSearchDiscoverer.discoverElasticSearchDomains(reloadConfig.region,reloadConfig.tags), RetryOperation.Config(3, 1000, 2)) match {
        case Success(result) =>
          if (result.nonEmpty) {
            LOGGER.info(s"Reloading(or loading) is successfully done for the configuration name =${observer.name}")
            observer.onReload(result.mkString(","))
          }
        case Failure(reason) =>
          LOGGER.error(s"Fail to reload the trace backend configuration for observer name=${observer.name}. " +
            s"Will try at next scheduled time", reason)
      }
    })
  }
}
