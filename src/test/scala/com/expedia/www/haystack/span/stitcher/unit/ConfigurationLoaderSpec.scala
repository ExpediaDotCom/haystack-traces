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
package com.expedia.www.haystack.span.stitcher.unit

import com.expedia.www.haystack.span.stitcher.config.ProjectConfiguration
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConversions._

class ConfigurationLoaderSpec extends FunSpec with Matchers {

  describe("Configuration loader") {
    it("should load the stitch config only from base.conf") {
      val spanStitchConfig = ProjectConfiguration.stitchConfig
      spanStitchConfig.pollIntervalMillis shouldBe 1000L
      spanStitchConfig.streamsCloseTimeoutMillis shouldBe 300L
      spanStitchConfig.maxEntries shouldBe 10
      spanStitchConfig.stitchWindowMillis shouldBe 1000L
    }

    it("should load the kafka config from base.conf and one stream property from env variable") {
      val kafkaConfig = ProjectConfiguration.kafkaConfig
      kafkaConfig.autoOffsetReset shouldBe AutoOffsetReset.LATEST
      kafkaConfig.produceTopic shouldBe "stitch-span"
      kafkaConfig.consumeTopic shouldBe "traces"
      kafkaConfig.streamsConfig.getList(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG).head shouldBe "kafka-svc:9092"
      kafkaConfig.streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG) shouldBe "haystack-span-stitch-app"
      kafkaConfig.streamsConfig.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG) shouldBe 4
      kafkaConfig.streamsConfig.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) shouldBe 500L
      kafkaConfig.changelogConfig.enabled shouldBe true
      kafkaConfig.changelogConfig.logConfig.get("retention.bytes") shouldBe "104857600"
      kafkaConfig.changelogConfig.logConfig.get("retention.ms") shouldBe "86400"
    }
  }
}
