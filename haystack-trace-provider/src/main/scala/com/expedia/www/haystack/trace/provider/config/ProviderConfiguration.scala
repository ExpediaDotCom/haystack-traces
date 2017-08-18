/*
 * Copyright 2017 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.expedia.www.haystack.trace.provider.config

import com.expedia.www.haystack.trace.provider.config.entities.{CassandraConfiguration, ElasticSearchConfiguration, ServiceConfiguration}
import com.typesafe.config.Config

object ProviderConfiguration {
  private val config: Config = ConfigurationLoader.loadAppConfig

  def serviceConfig: ServiceConfiguration = {
    val serviceConfig: Config = config.getConfig("service")
    ServiceConfiguration(serviceConfig.getInt("port"))
  }

  def cassandraConfig: CassandraConfiguration = {
    val cassandraConfig: Config = config.getConfig("cassandra")
    CassandraConfiguration(cassandraConfig.getString("endpoint"))
  }

  def elasticSearchConfig: ElasticSearchConfiguration = {
    val elasticSearchConfig: Config = config.getConfig("elasticsearch")
    ElasticSearchConfiguration(elasticSearchConfig.getString("endpoint"))
  }
}
