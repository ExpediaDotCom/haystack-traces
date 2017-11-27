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

package com.expedia.www.haystack.trace.commons.config.reload

import com.expedia.www.haystack.trace.commons.config.entities.ReloadConfiguration
import com.expedia.www.haystack.trace.commons.retries.RetryOperation
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.Search

import scala.util.{Failure, Success}

class ConfigurationReloadElasticSearchProvider(reloadConfig: ReloadConfiguration)
  extends ConfigurationReloadProvider(reloadConfig) {

  private val matchAllQuery = "{\"query\":{\"match_all\":{\"boost\":1.0}}}"

  private val esClient: JestClient = {
    val factory = new JestClientFactory()
    factory.setHttpClientConfig(new HttpClientConfig.Builder(reloadConfig.configStoreEndpoint).build())
    factory.getObject
  }

  /**
    * loads the configuration from external store
    */
  override def load(): Unit = {
    reloadConfig.observers.foreach(observer => {

      val searchQuery = new Search.Builder(matchAllQuery)
        .addIndex(reloadConfig.databaseName)
        .addType(observer.name)
        .build()

      RetryOperation.executeWithRetryBackoff(() => esClient.execute(searchQuery), RetryOperation.Config(3, 1000, 2)) match {
        case Success(result) =>
          if (result.isSucceeded) {
            LOGGER.info(s"Reloading(or loading) is successfully done for the configuration name =${observer.name}")
            observer.onReload(result.getSourceAsString)
          } else {
            LOGGER.error(s"Fail to reload the configuration from elastic search with error: ${result.getErrorMessage} " +
              s"for observer name=${observer.name}")
          }

        case Failure(reason) =>
          LOGGER.error(s"Fail to reload the configuration from elastic search for observer name=${observer.name}. " +
            s"Will try at next scheduled time", reason)
      }
    })
  }

  override def close(): Unit = {
    this.esClient.shutdownClient()
    super.close()
  }
}
