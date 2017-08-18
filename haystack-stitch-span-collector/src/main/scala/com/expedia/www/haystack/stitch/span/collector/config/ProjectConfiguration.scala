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

package com.expedia.www.haystack.stitch.span.collector.config

import com.datastax.driver.core.ConsistencyLevel
import com.expedia.www.haystack.stitch.span.collector.config.entities._
import com.expedia.www.haystack.stitch.span.collector.config.reload.{ConfigurationReloadElasticSearchProvider, Reloadable}
import com.typesafe.config.Config

import scala.collection.JavaConversions._
import scala.util.Try

class ProjectConfiguration extends AutoCloseable {
  val config: Config = ConfigurationLoader.loadAppConfig

  /**
    * collector specific configuration like parallelism, write batch-size, batch-interval
    */
  lazy val collectorConfig: CollectorConfiguration = {
    val collector = config.getConfig("collector")
    CollectorConfiguration(
      collector.getInt("parallelism"),
      collector.getString("topic"),
      collector.getInt("write.batch.size"),
      collector.getInt("write.batch.timeout.ms"),
      collector.getInt("commit.batch.size"))
  }

  /**
    *
    * cassandra configuration object
    */
  lazy val cassandraConfig: CassandraConfiguration = {
    val cs = config.getConfig("cassandra")

    val awsConfig =
      if(cs.hasPath("auto.discovery.aws")) {
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

    val consistencyLevel = ConsistencyLevel.values().find(_.toString.equalsIgnoreCase(cs.getString("consistency.level"))).get

    CassandraConfiguration(
      if(cs.hasPath("endpoints")) cs.getString("endpoints").split(",").toList else Nil,
      cs.getBoolean("auto.discovery.enabled"),
      awsConfig,
      cs.getString("keyspace.name"),
      cs.getString("keyspace.table.name"),
      cs.getBoolean("keyspace.auto.create"),
      consistencyLevel,
      cs.getInt("ttl.sec"),
      socket)
  }

  /**
    *
    * elastic search configuration object
    */
  lazy val elasticSearchConfig: ElasticSearchConfiguration = {
    val es = config.getConfig("elasticsearch")
    ElasticSearchConfiguration(
      endpoint = es.getString("endpoint"),
      consistencyLevel = es.getString("consistency.level"),
      indexNamePrefix = es.getString("index.name.prefix"),
      indexType = es.getString("index.type"),
      es.getInt("conn.timeout.ms"),
      es.getInt("read.timeout.ms"))
  }

  val indexConfig: IndexConfiguration = {
    val indexConfig = IndexConfiguration(Nil)
    indexConfig.reloadConfigTableName = config.getConfig("reload.tables").getString("index.fields.config")
    indexConfig
  }

  private val reloader = registerReloadableConfigurations(List(indexConfig))

  private def registerReloadableConfigurations(observers: Seq[Reloadable]): ConfigurationReloadElasticSearchProvider = {
    val reload = config.getConfig("reload")
    val reloadConfig = ReloadConfiguration(
      reload.getString("config.endpoint"),
      reload.getString("config.database.name"),
      reload.getInt("interval.ms"),
      observers,
      loadOnStartup = reload.getBoolean("startup.load"))

    new ConfigurationReloadElasticSearchProvider(reloadConfig)
  }

  override def close(): Unit = {
    Try(reloader.close())
  }
}
