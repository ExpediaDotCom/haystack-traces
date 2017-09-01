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

package com.expedia.www.haystack.trace.indexer.unit

import com.datastax.driver.core.ConsistencyLevel
import com.expedia.www.haystack.trace.indexer.config.ProjectConfiguration
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConversions._

class ConfigurationLoaderSpec extends FunSpec with Matchers {

  val project = new ProjectConfiguration()
  describe("Configuration loader") {
    it("should load the span buffer config only from base.conf") {
      val config = project.spanAccumulateConfig
      config.pollIntervalMillis shouldBe 1000L
      config.maxEntriesAllStores shouldBe 20000
      config.bufferingWindowMillis shouldBe 1000L
    }

    it("should load the kafka config from base.conf and one stream property from env variable") {
      val kafkaConfig = project.kafkaConfig
      kafkaConfig.autoOffsetReset shouldBe AutoOffsetReset.LATEST
      kafkaConfig.produceTopic shouldBe "span-buffer"
      kafkaConfig.consumeTopic shouldBe "spans"
      kafkaConfig.streamsConfig.getList(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG).head shouldBe "kafka-svc:9092"
      kafkaConfig.streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG) shouldBe "haystack-trace-indexer"
      kafkaConfig.streamsConfig.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG) shouldBe 4
      kafkaConfig.streamsConfig.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) shouldBe 500L
      kafkaConfig.changelogConfig.enabled shouldBe true
      kafkaConfig.changelogConfig.logConfig.get("retention.bytes") shouldBe "104857600"
      kafkaConfig.changelogConfig.logConfig.get("retention.ms") shouldBe "86400"
      kafkaConfig.streamsCloseTimeoutInMillis shouldBe 300
    }

    it("should load the cassandra config from base.conf and few properties overridden from env variable") {
      val cassandra = project.cassandraConfig
      cassandra.consistencyLevel shouldEqual ConsistencyLevel.ONE
      cassandra.autoDiscoverEnabled shouldBe false
      // this will fail if run inside an editor, we override this config using env variable inside pom.xml
      cassandra.endpoints should contain allOf("cass1", "cass2")
      cassandra.autoCreateSchema shouldBe Some("cassandra_cql_schema")
      cassandra.awsNodeDiscovery shouldBe empty
      cassandra.socket.keepAlive shouldBe true
      cassandra.socket.maxConnectionPerHost shouldBe 100
      cassandra.socket.readTimeoutMills shouldBe 5000
      cassandra.socket.connectionTimeoutMillis shouldBe 10000
      cassandra.recordTTLInSec shouldBe 86400
      cassandra.maxInFlightRequests shouldBe 100
    }

    it("should load the elastic search config from base.conf and one property overridden from env variable") {
      val elastic = project.elasticSearchConfig
      elastic.endpoint shouldBe "http://elasticsearch:9200"
      elastic.maxInFlightRequests shouldBe 50
      elastic.indexTemplateJson shouldBe Some("some_template_json")
      elastic.consistencyLevel shouldBe "one"
      elastic.readTimeoutMillis shouldBe 5000
      elastic.connectionTimeoutMillis shouldBe 10000
      elastic.indexNamePrefix shouldBe "haystack-test"
      elastic.indexType shouldBe "spans"
    }
  }
}
