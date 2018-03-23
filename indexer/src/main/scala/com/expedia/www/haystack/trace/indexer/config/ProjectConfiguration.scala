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
import com.datastax.driver.core.exceptions.DriverException
import com.expedia.www.haystack.trace.commons.config.ConfigurationLoader
import com.expedia.www.haystack.trace.commons.config.entities._
import com.expedia.www.haystack.trace.commons.config.reload.{ConfigurationReloadElasticSearchProvider, Reloadable}
import com.expedia.www.haystack.trace.commons.retries.RetryOperation
import com.expedia.www.haystack.trace.indexer.config.entities._
import com.expedia.www.haystack.trace.indexer.serde.SpanDeserializer
import com.typesafe.config.Config
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringDeserializer, StringSerializer}

import scala.collection.JavaConversions._
import scala.util.Try

class ProjectConfiguration extends AutoCloseable {
  private val config = ConfigurationLoader.loadAppConfig

  val healthStatusFilePath: String = config.getString("health.status.path")

  /**
    * span accumulation related configuration like max buffered records, buffer window, poll interval
    *
    * @return a span config object
    */
  val spanAccumulateConfig: SpanAccumulatorConfiguration = {
    val cfg = config.getConfig("span.accumulate")
    SpanAccumulatorConfiguration(
      cfg.getInt("store.min.traces.per.cache"),
      cfg.getInt("store.all.max.entries"),
      cfg.getLong("poll.ms"),
      cfg.getLong("window.ms"))
  }

  /**
    *
    * @return streams configuration object
    */
  val kafkaConfig: KafkaConfiguration = {
    // verify if the applicationId and bootstrap server config are non empty
    def verifyAndUpdateConsumerProps(props: Properties): Unit = {
      require(props.getProperty(ConsumerConfig.GROUP_ID_CONFIG).nonEmpty)
      require(props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).nonEmpty)

      // make sure auto commit is false
      require(props.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) == "false")

      // set the deserializers explicitly
      props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
      props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new SpanDeserializer().getClass.getCanonicalName)
    }

    def verifyAndUpdateProducerProps(props: Properties): Unit = {
      require(props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).nonEmpty)
      props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
      props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getCanonicalName)
    }

    def addProps(config: Config, props: Properties): Unit = {
      if (config != null) {
        config.entrySet() foreach {
          kv => {
            props.setProperty(kv.getKey, kv.getValue.unwrapped().toString)
          }
        }
      }
    }

    val kafka = config.getConfig("kafka")
    val producerConfig = if (kafka.hasPath("producer")) kafka.getConfig("producer") else null
    val consumerConfig = kafka.getConfig("consumer")

    val consumerProps = new Properties
    val producerProps = new Properties

    // producer specific properties
    addProps(producerConfig, producerProps)

    // consumer specific properties
    addProps(consumerConfig, consumerProps)

    // validate consumer props
    verifyAndUpdateConsumerProps(consumerProps)
    verifyAndUpdateProducerProps(producerProps)

    KafkaConfiguration(
      numStreamThreads = kafka.getInt("num.stream.threads"),
      pollTimeoutMs = kafka.getLong("poll.timeout.ms"),
      consumerProps = consumerProps,
      producerProps = producerProps,
      produceTopic = if (kafka.hasPath("topic.produce")) kafka.getString("topic.produce") else "",
      consumeTopic = kafka.getString("topic.consume"),
      consumerCloseTimeoutInMillis = kafka.getInt("close.stream.timeout.ms"),
      commitOffsetRetries = kafka.getInt("commit.offset.retries"),
      commitBackoffInMillis = kafka.getLong("commit.offset.backoff.ms"),
      maxWakeups = kafka.getInt("max.wakeups"),
      wakeupTimeoutInMillis = kafka.getInt("wakeup.timeout.ms"))
  }

  /**
    *
    * cassandra configuration object
    */
  val cassandraWriteConfig: CassandraWriteConfiguration = {

    def toConsistencyLevel(level: String) = ConsistencyLevel.values().find(_.toString.equalsIgnoreCase(level)).get

    def consistencyLevelOnErrors(cs: Config) = {
      val consistencyLevelOnErrors = cs.getStringList("on.error.consistency.level")
      val consistencyLevelOnErrorList = scala.collection.mutable.ListBuffer[(Class[_], ConsistencyLevel)]()

      var idx = 0
      while(idx < consistencyLevelOnErrors.size()) {
        val errorClass = consistencyLevelOnErrors.get(idx)
        val level = consistencyLevelOnErrors.get(idx + 1)
        consistencyLevelOnErrorList.+=((Class.forName(errorClass), toConsistencyLevel(level)))
        idx = idx + 2
      }

      consistencyLevelOnErrorList.toList
    }

    val cs = config.getConfig("cassandra")

    val awsConfig: Option[AwsNodeDiscoveryConfiguration] =
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

    val consistencyLevel = toConsistencyLevel(cs.getString("consistency.level"))

    val keyspaceConfig = cs.getConfig("keyspace")

    val autoCreateSchemaField = "auto.create.schema"
    val autoCreateSchema = if (keyspaceConfig.hasPath(autoCreateSchemaField)
      && StringUtils.isNotEmpty(keyspaceConfig.getString(autoCreateSchemaField))) {
      Some(keyspaceConfig.getString(autoCreateSchemaField))
    } else {
      None
    }


    CassandraWriteConfiguration(
      clientConfig = CassandraConfiguration(
        if (cs.hasPath("endpoints")) cs.getString("endpoints").split(",").toList else Nil,
        cs.getBoolean("auto.discovery.enabled"),
        awsConfig,
        credentialsConfig,
        keyspaceConfig.getString("name"),
        keyspaceConfig.getString("table.name"),
        autoCreateSchema,
        socket),
      consistencyLevel = consistencyLevel,
      recordTTLInSec = cs.getInt("ttl.sec"),
      maxInFlightRequests = cs.getInt("max.inflight.requests"),
      retryConfig = RetryOperation.Config(
        cs.getInt("retries.max"),
        cs.getLong("retries.backoff.initial.ms"),
        cs.getDouble("retries.backoff.factor")),
      consistencyLevelOnErrors(cs))
  }

  /**
    *
    * elastic search configuration object
    */
  val elasticSearchConfig: ElasticSearchConfiguration = {
    val es = config.getConfig("elasticsearch")
    val indexConfig = es.getConfig("index")

    val templateJsonConfigField = "template.json"
    val indexTemplateJson = if (indexConfig.hasPath(templateJsonConfigField)
      && StringUtils.isNotEmpty(indexConfig.getString(templateJsonConfigField))) {
      Some(indexConfig.getString(templateJsonConfigField))
    } else {
      None
    }
    val ausername = if (es.hasPath("username")){Option(es.getString("username"))}else{None}
    val apassword = if (es.hasPath("password")){Option(es.getString("password"))}else{None}

    ElasticSearchConfiguration(
      endpoint = es.getString("endpoint"),
      username = ausername,
      password = apassword,
      indexTemplateJson,
      consistencyLevel = es.getString("consistency.level"),
      indexNamePrefix = indexConfig.getString("name.prefix"),
      indexHourBucket = indexConfig.getInt("hour.bucket"),
      indexType = indexConfig.getString("type"),
      connectionTimeoutMillis = es.getInt("conn.timeout.ms"),
      readTimeoutMillis = es.getInt("read.timeout.ms"),
      maxInFlightBulkRequests = es.getInt("bulk.max.inflight"),
      maxDocsInBulk = es.getInt("bulk.max.docs.count"),
      maxBulkDocSizeInBytes = es.getInt("bulk.max.docs.size.kb") * 1000,
      retryConfig = RetryOperation.Config(
        es.getInt("retries.max"),
        es.getLong("retries.backoff.initial.ms"),
        es.getDouble("retries.backoff.factor")))
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
  private val reloader = registerReloadableConfigurations(List(indexConfig))

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
      if(reload.hasPath("config.username")){Option(reload.getString("config.username"))}else{None},
      if(reload.hasPath("config.password")){Option(reload.getString("config.password"))}else{None},
      observers,
      loadOnStartup = reload.getBoolean("startup.load"))

    val loader = new ConfigurationReloadElasticSearchProvider(reloadConfig)
    if (reloadConfig.loadOnStartup) loader.load()
    loader
  }

  override def close(): Unit = {
    Try(reloader.close())
  }
}
