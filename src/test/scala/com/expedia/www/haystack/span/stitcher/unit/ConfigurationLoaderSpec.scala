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
      spanStitchConfig.loggingEnabled shouldBe false
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
    }
  }
}
