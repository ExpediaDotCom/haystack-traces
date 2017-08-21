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
package com.expedia.www.haystack.stitched.span.collector.unit

import com.datastax.driver.core.ConsistencyLevel
import com.expedia.www.haystack.stitched.span.collector.config.ProjectConfiguration
import org.scalatest.{FunSpec, Matchers}

class ConfigurationLoaderSpec extends FunSpec with Matchers {

  describe("Configuration loader") {
    val project = new ProjectConfiguration()

    it("should load the collector config only from base.conf") {
      val collector = new ProjectConfiguration().collectorConfig
      collector.batchSize shouldBe 250
      collector.batchTimeoutMillis shouldBe 250
      collector.consumerTopic shouldBe "stitch-spans"
      collector.parallelism shouldBe 2
      collector.commitBatch shouldBe 1000
    }

    it("should load the cassandra config from base.conf and few properties overridden from env variable") {
      val cassandra = project.cassandraConfig
      cassandra.consistencyLevel shouldEqual ConsistencyLevel.ONE
      cassandra.autoDiscoverEnabled shouldBe false
      cassandra.endpoints should contain allOf("cass1", "cass2")
      cassandra.autoCreateKeyspace shouldBe true
      cassandra.awsNodeDiscovery shouldBe empty
      cassandra.socket.keepAlive shouldBe true
      cassandra.socket.maxConnectionPerHost shouldBe 100
      cassandra.socket.readTimeoutMills shouldBe 5000
      cassandra.socket.connectionTimeoutMillis shouldBe 10000
      cassandra.recordTTLInSec shouldBe 86400
    }

    it("should load the elastic search config from base.conf and one property overridden from env variable") {
      val elastic = project.elasticSearchConfig
      elastic.endpoint shouldBe "http://elasticsearch:9200"
      elastic.consistencyLevel shouldBe "one"
      elastic.readTimeoutMillis shouldBe 5000
      elastic.connectionTimeoutMillis shouldBe 10000
      elastic.indexNamePrefix shouldBe "haystack-test"
      elastic.indexType shouldBe "spans"
    }

    it("should load akka config from base.conf and one property overridden from env variable") {
      val akka = project.config.getConfig("akka")
      akka should not be null
      val kafkaClients = akka.getConfig("kafka.consumer").getConfig("kafka-clients")
      kafkaClients should not be null
      kafkaClients.getBoolean("enable.auto.commit") shouldBe true
    }
  }
}