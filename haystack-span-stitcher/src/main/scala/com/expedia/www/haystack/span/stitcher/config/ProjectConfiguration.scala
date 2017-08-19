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
package com.expedia.www.haystack.span.stitcher.config

import java.util.Properties

import com.expedia.www.haystack.span.stitcher.config.entities.{ChangelogConfiguration, KafkaConfiguration, StitchConfiguration}
import com.typesafe.config.Config
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset

import scala.collection.JavaConversions._

object ProjectConfiguration {
  private val config = ConfigurationLoader.loadAppConfig

  /**
    * span-stitch related configuration like max stitch records, stitching window, poll interval for stitch operation
    * @return a span config object
    */
  def stitchConfig: StitchConfiguration = {
    val cfg = config.getConfig("span.stitch")
    StitchConfiguration(
      cfg.getInt("store.initial.size"),
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
  def kafkaConfig: KafkaConfiguration = {

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

    val timestampExtractor = Class.forName(props.getProperty("timestamp.extractor",
      "org.apache.kafka.streams.processor.WallclockTimestampExtractor"))

    KafkaConfiguration(new StreamsConfig(props),
      produceTopic = producerConfig.getString("topic"),
      consumeTopic = consumerConfig.getString("topic"),
      offsetReset,
      timestampExtractor.newInstance().asInstanceOf[TimestampExtractor],
      changelogConfig)
  }
}