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

import com.expedia.www.haystack.trace.provider.config.entities._
import com.typesafe.config.Config

import scala.collection.JavaConversions._

object ProviderConfiguration {
  private val config: Config = ConfigurationLoader.loadAppConfig

  lazy val serviceConfig: ServiceConfiguration = {
    val serviceConfig: Config = config.getConfig("service")
    ServiceConfiguration(serviceConfig.getInt("port"))
  }

  lazy val cassandraConfig: CassandraConfiguration = {
    val cs = config.getConfig("cassandra")

    val awsConfig =
      if (cs.hasPath("auto.discovery.aws")) {
        val aws = cs.getConfig("auto.discovery.aws")
        val tags = aws.getConfig("tags")
          .entrySet()
          .map(elem => elem.getKey -> elem.getValue.unwrapped().toString)
          .toMap
        Some(AwsNodeDiscoveryConfiguration(aws.getString("region"), tags))
      } else {
        None
      }

    val socketConfig = cs.getConfig("connections")

    val socket = SocketConfiguration(
      socketConfig.getInt("max.per.host"),
      socketConfig.getBoolean("keep.alive"),
      socketConfig.getInt("conn.timeout.ms"),
      socketConfig.getInt("read.timeout.ms"))

    CassandraConfiguration(
      if (cs.hasPath("endpoints")) cs.getString("endpoints").split(",").toList else Nil,
      cs.getBoolean("auto.discovery.enabled"),
      awsConfig,
      cs.getString("keyspace.name"),
      cs.getString("keyspace.table.name"),
      socket)
  }

  lazy val elasticSearchConfig: ElasticSearchConfiguration =  {
    val es = config.getConfig("elasticsearch")
    val indexConfig = es.getConfig("index")

    ElasticSearchConfiguration(
      endpoint = es.getString("endpoint"),
      indexNamePrefix = indexConfig.getString("name.prefix"),
      indexType = indexConfig.getString("type"),
      es.getInt("conn.timeout.ms"),
      es.getInt("read.timeout.ms"))
  }
}
