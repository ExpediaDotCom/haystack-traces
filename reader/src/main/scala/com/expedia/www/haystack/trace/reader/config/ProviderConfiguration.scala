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

package com.expedia.www.haystack.trace.reader.config

import java.util

import com.expedia.www.haystack.commons.config.ConfigurationLoader
import com.expedia.www.haystack.trace.commons.config.entities._
import com.expedia.www.haystack.trace.commons.config.reload.{ConfigurationReloadElasticSearchProvider, Reloadable}
import com.expedia.www.haystack.trace.reader.config.entities._
import com.expedia.www.haystack.trace.reader.readers.transformers.TraceTransformer
import com.expedia.www.haystack.trace.reader.readers.validators.TraceValidator
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class ProviderConfiguration {
  private val config: Config = ConfigurationLoader.loadConfigFileWithEnvOverrides()

  val healthStatusFilePath: String = config.getString("health.status.path")

  val serviceConfig: ServiceConfiguration = {
    val serviceConfig = config.getConfig("service")

    val ssl = serviceConfig.getConfig("ssl")
    val sslConfig = SslConfiguration(ssl.getBoolean("enabled"), ssl.getString("cert.path"), ssl.getString("private.key.path"))

    ServiceConfiguration(serviceConfig.getInt("port"), sslConfig)
  }

  /**
    * Cassandra configuration object
    */
  val cassandraConfig: CassandraConfiguration = {
    val cs = config.getConfig("cassandra")

    val awsConfig =
      if (cs.hasPath("auto.discovery.aws")) {
        val aws = cs.getConfig("auto.discovery.aws")
        val tags = aws.getConfig("tags")
          .entrySet()
          .asScala
          .map(elem => elem.getKey -> elem.getValue.unwrapped().toString)
          .toMap
        Some(AwsNodeDiscoveryConfiguration(aws.getString("region"), tags))
      } else {
        None
      }
    val credentialsConfig: Option[CredentialsConfiguration] =
      if (cs.hasPath("credentials")) {
        Some(CredentialsConfiguration(cs.getString("credentials.username"), cs.getString("credentials.password")))
      } else {
        None
      }

    val socketConfig = cs.getConfig("connections")

    val socket = SocketConfiguration(
      socketConfig.getInt("max.per.host"),
      socketConfig.getBoolean("keep.alive"),
      socketConfig.getInt("conn.timeout.ms"),
      socketConfig.getInt("read.timeout.ms"))

    val tracesKeyspace = cs.getConfig("keyspace")

    CassandraConfiguration(
      if (cs.hasPath("endpoints")) cs.getString("endpoints").split(",").toList else Nil,
      cs.getBoolean("auto.discovery.enabled"),
      awsConfig,
      credentialsConfig,
      KeyspaceConfiguration(tracesKeyspace.getString("name"), tracesKeyspace.getString("table.name"), -1),
      socket)
  }

  val serviceMetadataConfig: ServiceMetadataReadConfiguration = {
    val metadataCfg = config.getConfig("service.metadata")
    val keyspace = metadataCfg.getConfig("cassandra.keyspace")

    ServiceMetadataReadConfiguration(
      metadataCfg.getBoolean("enabled"),
      KeyspaceConfiguration(keyspace.getString("name"), keyspace.getString("table.name")))
  }

  /**
    * ElasticSearch configuration
    */
  val elasticSearchConfig: ElasticSearchConfiguration = {
    val es = config.getConfig("elasticsearch")
    val indexConfig = es.getConfig("index")

    val ausername = if (es.hasPath("username")) { Option(es.getString("username")) } else None
    val apassword = if (es.hasPath("password")) { Option(es.getString("password")) } else None

    ElasticSearchConfiguration(
      endpoint = es.getString("endpoint"),
      username = ausername,
      password = apassword,
      indexNamePrefix = indexConfig.getString("name.prefix"),
      indexType = indexConfig.getString("type"),
      es.getInt("conn.timeout.ms"),
      es.getInt("read.timeout.ms"),
      indexHourBucket = indexConfig.getInt("hour.bucket"),
      indexHourTtl = indexConfig.getInt("hour.ttl"))
  }

  private def toInstances[T](classes: util.List[String])(implicit ct: ClassTag[T]): scala.Seq[T] = {
    classes.asScala.map(className => {
      val c = Class.forName(className)

      if (c == null) {
        throw new RuntimeException(s"No class found with name $className")
      } else {
        val o = c.newInstance()
        val baseClass = ct.runtimeClass

        if (!baseClass.isInstance(o)) {
          throw new RuntimeException(s"${c.getName} is not an instance of ${baseClass.getName}")
        }
        o.asInstanceOf[T]
      }
    })
  }

  /**
    * Configurations to specify what all transforms to apply on traces
    */
  val traceTransformerConfig: TraceTransformersConfiguration = {
    val preTransformers = config.getStringList("trace.transformers.pre.sequence")
    val postTransformers = config.getStringList("trace.transformers.post.sequence")

    TraceTransformersConfiguration(
      toInstances[TraceTransformer](preTransformers),
      toInstances[TraceTransformer](postTransformers))
  }

  /**
    * Configurations to specify what all validations to apply on traces
    */
  val traceValidatorConfig: TraceValidatorsConfiguration = {
    val validatorConfig: Config = config.getConfig("trace.validators")
    TraceValidatorsConfiguration(toInstances[TraceValidator](validatorConfig.getStringList("sequence")))
  }

  /**
    * configuration that contains list of tags that should be indexed for a span
    */
  val indexConfig: WhitelistIndexFieldConfiguration = {
    val indexConfig = WhitelistIndexFieldConfiguration()
    indexConfig.reloadConfigTableName = Option(config.getConfig("reload.tables").getString("index.fields.config"))
    indexConfig
  }

  // configuration reloader
  registerReloadableConfigurations(List(indexConfig))

  /**
    * registers a reloadable config object to reloader instance.
    * The reloader registers them as observers and invokes them periodically when it re-reads the
    * configuration from an external store
    *
    * @param observers list of reloadable configuration objects
    * @return the reloader instance that uses ElasticSearch as an external database for storing the configs
    */
  private def registerReloadableConfigurations(observers: Seq[Reloadable]): ConfigurationReloadElasticSearchProvider = {
    val reload = config.getConfig("reload")
    val reloadConfig = ReloadConfiguration(
      reload.getString("config.endpoint"),
      reload.getString("config.database.name"),
      reload.getInt("interval.ms"),
      if(reload.hasPath("config.username")){ Option(reload.getString("config.username")) } else None,
      if(reload.hasPath("config.password")){ Option(reload.getString("config.password")) } else None,
      observers,
      loadOnStartup = reload.getBoolean("startup.load"))

    val loader = new ConfigurationReloadElasticSearchProvider(reloadConfig)
    if (reloadConfig.loadOnStartup) loader.load()
    loader
  }
}
