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

package com.expedia.www.haystack.trace.indexer.integration

import java.util

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.indexer.StreamTopology
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils

import scala.collection.JavaConversions._
import scala.concurrent.duration._

class FailedTopologyRecoverySpec extends BaseIntegrationTestSpec {
  private val MAX_CHILD_SPANS = 5
  private val TRACE_ID_1 = "traceid-1"
  private val SPAN_ID_PREFIX = "span-id-"
  private val TRACE_DESCRIPTIONS = List(TraceDescription(TRACE_ID_1, SPAN_ID_PREFIX))

  "Trace Indexing Topology" should {
    s"consume spans from input '${kafka.INPUT_TOPIC}', buffer them together keyed by unique TraceId and write to cassandra and elastic even if crashed in between" in {
      Given("a set of spans produced async with spanBuffer+kafka configurations")
      val kafkaConfig = kafka.buildConfig
      val esConfig = elastic.buildConfig
      val indexTagsConfig = elastic.indexingConfig
      val cassandraConfig = cassandra.buildConfig
      val accumulatorConfig = spanAccumulatorConfig.copy(pollIntervalMillis = spanAccumulatorConfig.pollIntervalMillis * 2)
      produceSpansAsync(
        MAX_CHILD_SPANS,
        produceInterval = 1.seconds,
        TRACE_DESCRIPTIONS,
        0,
        spanAccumulatorConfig.bufferingWindowMillis)

      When(s"kafka-streams topology is started and then stopped forcefully after few sec")
      var topology = new StreamTopology(kafkaConfig, accumulatorConfig, esConfig, cassandraConfig, indexTagsConfig)
      topology.start()
      Thread.sleep(6000)
      topology.close()

      Then(s"on restart of the topology, we should be able to read span-buffer object created in previous run from the '${kafka.OUTPUT_TOPIC}' topic")
      topology = new StreamTopology(kafkaConfig, accumulatorConfig, esConfig, cassandraConfig, indexTagsConfig)
      topology.start()

      // produce one more span record with same traceId to trigger punctuate
      produceSpansAsync(
        1,
        produceInterval = 1.seconds,
        TRACE_DESCRIPTIONS,
        startRecordTimestamp = spanAccumulatorConfig.bufferingWindowMillis,
        spanAccumulatorConfig.bufferingWindowMillis)

      val records: util.List[KeyValue[String, SpanBuffer]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(kafka.RESULT_CONSUMER_CONFIG, kafka.OUTPUT_TOPIC, 1, MAX_WAIT_FOR_OUTPUT_MS)

      validate(records, MAX_CHILD_SPANS)
      verifyCassandraWrites(TRACE_DESCRIPTIONS, MAX_CHILD_SPANS - 1, MAX_CHILD_SPANS)
      verifyElasticSearchWrites(Seq(TRACE_ID_1))

      topology.close()
    }
  }

  // validate the received records
  private def validate(records: util.List[KeyValue[String, SpanBuffer]], childSpanCount: Int) = {
    // expect only one span buffer
    records.size() shouldBe 1
    records.head.key shouldBe TRACE_ID_1
    records.head.value.getChildSpansCount should be >=childSpanCount
  }
}
