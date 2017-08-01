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

import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset

case class ProducerKafkaConfiguration(topic: String,
                                      maxBufferSize: Long,
                                      batchSize: Long,
                                      maxBlockMillis: Long,
                                      retries: Int)

case class ConsumerKafkaConfiguration(topic: String)

case class KafkaStreamsConfiguration(applicationId: String,
                                     bootstrapServers: String,
                                     numStreamThreads: Long,
                                     metadataMaxAgeMs: Long,
                                     reconnectBackOffMillis: Long,
                                     retryBackoffMillis: Long,
                                     commitIntervalMillis: Long,
                                     offsetResetPolicy: AutoOffsetReset,
                                     spanRecordingWindowMillis: Long,
                                     producerConfig: ProducerKafkaConfiguration,
                                     consumerConfig: ConsumerKafkaConfiguration)


object ProjectConfiguration {
  import ConfigurationLoader._

  private val config = loadAppConfig

  def kafkaStreamsConfig: KafkaStreamsConfiguration = {
    val kafka = config.getConfig("kafka")

    val producer = kafka.getConfig("producer")
    val producerConfig = ProducerKafkaConfiguration(
      topic = producer.getString("topic"),
      maxBufferSize = producer.getLong("buffer.memory"),
      batchSize = producer.getLong("batch.size"),
      maxBlockMillis = producer.getLong("max.block.ms"),
      retries = producer.getInt("retries"))

    val consumer = kafka.getConfig("consumer")
    val consumerConfig = ConsumerKafkaConfiguration(topic = consumer.getString("topic"))

    KafkaStreamsConfiguration(
      applicationId = kafka.getString("application.id"),
      bootstrapServers = kafka.getString("bootstrap.servers"),
      numStreamThreads = kafka.getLong("num.stream.threads"),
      metadataMaxAgeMs = kafka.getLong("metadata.max.age.ms"),
      reconnectBackOffMillis = kafka.getLong("reconnect.backoff.ms"),
      retryBackoffMillis = kafka.getLong("retry.backoff.ms"),
      commitIntervalMillis = kafka.getLong("commit.interval.ms"),
      offsetResetPolicy = AutoOffsetReset.valueOf(kafka.getString("auto.offset.reset").toUpperCase),
      spanRecordingWindowMillis = kafka.getLong("span.recording.window.ms"),
      producerConfig = producerConfig,
      consumerConfig = consumerConfig)
  }
}