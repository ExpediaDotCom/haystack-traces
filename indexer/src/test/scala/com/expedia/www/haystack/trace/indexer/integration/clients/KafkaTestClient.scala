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

package com.expedia.www.haystack.trace.indexer.integration.clients

import java.util.Properties

import com.expedia.www.haystack.trace.indexer.config.entities.{ChangelogConfiguration, KafkaConfiguration}
import com.expedia.www.haystack.trace.indexer.integration.serdes.{SpanBufferProtoDeserializer, SpanProtoSerializer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.integration.utils.{EmbeddedKafkaCluster, IntegrationTestUtils}
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset

object KafkaTestClient {
  val KAFKA_CLUSTER = new EmbeddedKafkaCluster(1)
  KAFKA_CLUSTER.start()
}

class KafkaTestClient {
  import KafkaTestClient._
  private val STREAMS_CONFIG = new Properties()
  private val CHANGELOG_CONSUMER_CONFIG = new Properties()

  val INPUT_TOPIC = "spans"
  val OUTPUT_TOPIC = "span-buffer"
//  private val CHANGELOG_TOPIC = s"$APP_ID-${StreamTopology.STATE_STORE_NAME}-changelog"

  val PRODUCER_CONFIG: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, "0")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[SpanProtoSerializer])
    props
  }

  val RESULT_CONSUMER_CONFIG = new Properties()

  def buildConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG),
    OUTPUT_TOPIC,
    INPUT_TOPIC,
    AutoOffsetReset.EARLIEST,
    new FailOnInvalidTimestamp,
    ChangelogConfiguration(enabled = true),
    3000)

  def prepare(appId: String): Unit = {
    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers)
    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, appId + "-result-consumer")
    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[SpanBufferProtoDeserializer])

    CHANGELOG_CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers)
    CHANGELOG_CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, appId + "-changelog-consumer")
    CHANGELOG_CONSUMER_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    CHANGELOG_CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    CHANGELOG_CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[SpanBufferProtoDeserializer])

    STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.bootstrapServers)
    STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
    STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
    STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "300")
    STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams")

    IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG)

    deleteTopics(INPUT_TOPIC, OUTPUT_TOPIC)
    KAFKA_CLUSTER.createTopic(INPUT_TOPIC, 2, 1)
    KAFKA_CLUSTER.createTopic(OUTPUT_TOPIC)
  }

  private def deleteTopics(topics: String*): Unit = KAFKA_CLUSTER.deleteTopicsAndWait(topics:_*)
}
