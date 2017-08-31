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

package com.expedia.www.haystack.trace.indexer.config

import java.util.Properties

import com.datastax.driver.core.ConsistencyLevel
import com.expedia.www.haystack.trace.indexer.config.reload.{ConfigurationReloadElasticSearchProvider, Reloadable}
import com.expedia.www.haystack.trace.indexer.config.entities._
import com.expedia.www.haystack.trace.indexer.config.reload.ConfigurationReloadElasticSearchProvider
import com.typesafe.config.Config
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset

import scala.collection.JavaConversions._
import scala.util.Try

class ProjectConfiguration extends AutoCloseable {
  private val config = ConfigurationLoader.loadAppConfig

  /**
    * span accumulation related configuration like max buffered records, buffer window, poll interval
    * @return a span config object
    */
  lazy val spanAccumulateConfig: SpanAccumulatorConfiguration = {
    val cfg = config.getConfig("span.accumulate")
    SpanAccumulatorConfiguration(
      cfg.getInt("store.min.traces.per.cache"),
      cfg.getInt("store.all.max.entries"),
      cfg.getLong("poll.ms"),
      cfg.getLong("window.ms"),
      cfg.getLong("streams.close.timeout.ms"))
  }

  private def changelogConfig: ChangelogConfiguration = {
    val cfg = config.getConfig("kafka.changelog")
    val enabled = cfg.getBoolean("enabled")
    val logConfigMap = new java.util.HashMap[String, String]()

    if(enabled && cfg.hasPath("logConfig")) {
      for(entry <- cfg.getConfig("logConfig").entrySet()) {
        logConfigMap.put(entry.getKey, entry.getValue.unwrapped().toString)
      }
    }

    ChangelogConfiguration(enabled, logConfigMap)
  }

  /**
    *
    * @return streams configuration object
    */
  lazy val kafkaConfig: KafkaConfiguration = {

    // verify if the applicationId and bootstrap server config are non empty
    def verifyRequiredProps(props: Properties): Unit = {
      require(props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG).nonEmpty)
      require(props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG).nonEmpty)
    }

    def addProps(config: Config, props: Properties, prefix: (String) => String = identity): Unit = {
      config.entrySet().foreach(kv => {
        val propKeyName = prefix(kv.getKey)
        props.setProperty(propKeyName, kv.getValue.unwrapped().toString)
      })
    }

    val kafka = config.getConfig("kafka")
    val producerConfig = kafka.getConfig("producer")
    val consumerConfig = kafka.getConfig("consumer")
    val streamsConfig = kafka.getConfig("streams")

    val props = new Properties

    // add stream specific properties
    addProps(streamsConfig, props)

    // producer specific properties
    addProps(producerConfig, props, (k) => StreamsConfig.producerPrefix(k))

    // consumer specific properties
    addProps(consumerConfig, props, (k) => StreamsConfig.consumerPrefix(k))

    // validate props
    verifyRequiredProps(props)

    val offsetReset = if(streamsConfig.hasPath("auto.offset.reset")) {
      AutoOffsetReset.valueOf(streamsConfig.getString("auto.offset.reset").toUpperCase)
    } else {
      AutoOffsetReset.LATEST
    }

    val timestampExtractor = Class.forName(props.getProperty("timestamp.extractor"))

    KafkaConfiguration(new StreamsConfig(props),
      produceTopic = producerConfig.getString("topic"),
      consumeTopic = consumerConfig.getString("topic"),
      offsetReset,
      timestampExtractor.newInstance().asInstanceOf[TimestampExtractor],
      changelogConfig)
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

    val keyspaceConfig = cs.getConfig("keyspace")

    val autoCreateSchemaField = "auto.create.schema"
    val autoCreateSchema = if(keyspaceConfig.hasPath(autoCreateSchemaField)
      && StringUtils.isNotEmpty(keyspaceConfig.getString(autoCreateSchemaField))) {
      Some(keyspaceConfig.getString(autoCreateSchemaField))
    } else {
      None
    }

    CassandraConfiguration(
      if(cs.hasPath("endpoints")) cs.getString("endpoints").split(",").toList else Nil,
      cs.getBoolean("auto.discovery.enabled"),
      awsConfig,
      keyspaceConfig.getString("name"),
      keyspaceConfig.getString("table.name"),
      autoCreateSchema,
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
    val indexConfig = es.getConfig("index")

    val templateJsonConfigField = "template.json"
    val indexTemplateJson = if(indexConfig.hasPath(templateJsonConfigField)
      && StringUtils.isNotEmpty(indexConfig.getString(templateJsonConfigField))) {
      Some(indexConfig.getString(templateJsonConfigField))
    } else {
      None
    }

    ElasticSearchConfiguration(
      endpoint = es.getString("endpoint"),
      indexTemplateJson,
      consistencyLevel = es.getString("consistency.level"),
      indexNamePrefix = indexConfig.getString("name.prefix"),
      indexType = indexConfig.getString("type"),
      es.getInt("conn.timeout.ms"),
      es.getInt("read.timeout.ms"))
  }

  /**
    * configuration that contains list of tags that should be indexed for a span
    */
  val indexConfig: IndexConfiguration = {
    val indexConfig = IndexConfiguration(Nil)
    indexConfig.reloadConfigTableName = config.getConfig("reload.tables").getString("index.fields.config")
    indexConfig
  }

  // configuration reloader
  private val reloader = registerReloadableConfigurations(List(indexConfig))

  /**
    * registers a reloadable config object to reloader instance.
    * The reloader registers them as observers and invokes them periodically when it re-reads the
    * configuration from an external store
    * @param observers list of reloadable configuration objects
    * @return the reloader instance that uses ElasticSearch as an external database for storing the configs
    */
  private def registerReloadableConfigurations(observers: Seq[Reloadable]): ConfigurationReloadElasticSearchProvider = {
    val reload = config.getConfig("reload")
    val reloadConfig = ReloadConfiguration(
      reload.getString("config.endpoint"),
      reload.getString("config.database.name"),
      reload.getInt("interval.ms"),
      observers,
      loadOnStartup = reload.getBoolean("startup.load"))

    val loader = new ConfigurationReloadElasticSearchProvider(reloadConfig)
    if(reloadConfig.loadOnStartup) loader.load()
    loader
  }

  override def close(): Unit = {
    Try(reloader.close())
  }
}
