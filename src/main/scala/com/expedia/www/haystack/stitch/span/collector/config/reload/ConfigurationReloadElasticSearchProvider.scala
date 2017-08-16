/*
 *  Copyright 2017 Expedia, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.expedia.www.haystack.stitch.span.collector.config.reload

import com.expedia.www.haystack.stitch.span.collector.config.entities.ReloadConfiguration
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.Search
import org.slf4j.LoggerFactory

class ConfigurationReloadElasticSearchProvider(reloadConfig: ReloadConfiguration)
  extends ConfigurationReloadProvider(reloadConfig) {

  private val LOGGER = LoggerFactory.getLogger(classOf[ConfigurationReloadElasticSearchProvider])

  private val matchAllQuery = "{\"query\":{\"match_all\":{\"boost\":1.0}}}"

  private val esClient: JestClient = {
    val factory = new JestClientFactory()
    factory.setHttpClientConfig(new HttpClientConfig.Builder(reloadConfig.configStoreEndpoint).build())
    factory.getObject
  }

  // load on startup
  if(reloadConfig.loadOnStartup) load()

  override def load(): Unit = {
    reloadConfig.observers.foreach(observer => {
      val searchQuery = new Search.Builder(matchAllQuery)
        .addIndex(reloadConfig.databaseName)
        .addType(observer.name)
        .build()

      val result = esClient.execute(searchQuery)
      if(result.isSucceeded) {
        LOGGER.info(s"Reloading(or loading) is successfully done for the configuration name =${observer.name}")
        observer.onReload(result.getSourceAsString)
      } else {
        LOGGER.error(s"Fail to reload the configuration from elastic search with error: ${result.getErrorMessage} " +
          s"for observer name=${observer.name}")
      }
    })
  }

  override def close(): Unit = {
    this.esClient.shutdownClient()
    super.close()
  }
}
