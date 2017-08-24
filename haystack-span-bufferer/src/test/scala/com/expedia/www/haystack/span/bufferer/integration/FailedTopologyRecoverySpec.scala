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
package com.expedia.www.haystack.span.bufferer.integration

import java.util

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.span.bufferer.StreamTopology
import com.expedia.www.haystack.span.bufferer.config.entities.{ChangelogConfiguration, KafkaConfiguration, SpanBufferConfiguration}
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
import org.apache.kafka.streams.{KeyValue, StreamsConfig}

import scala.collection.JavaConversions._
import scala.concurrent.duration._

class FailedTopologyRecoverySpec extends BaseIntegrationTestSpec {
  private val MAX_CHILD_SPANS = 5
  private val TRACE_ID_1 = "traceid-1"
  private val SPAN_ID_PREFIX = "span-id-"

  "Span Buffering Topology" should {
    s"consume spans from input '$INPUT_TOPIC' and buffered them together keyed by unique TraceId" in {
      Given("a set of spans produced async with spanBuffer+kafka configurations")
      val spanBufferConfig = SpanBufferConfiguration(
        INITIAL_STORE_CAPACITY,
        MAX_SPAN_BUFFERS_IN_MEM,
        PUNCTUATE_INTERVAL_MS * 2,
        SPAN_BUFFERING_WINDOW_MS,
        AUTO_COMMIT_INTERVAL_MS)
      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG),
        OUTPUT_TOPIC,
        INPUT_TOPIC,
        AutoOffsetReset.EARLIEST,
        new FailOnInvalidTimestamp,
        ChangelogConfiguration(enabled = true))

      produceSpansAsync(MAX_CHILD_SPANS,
        produceInterval = 1.seconds,
        List(SpanDescription(TRACE_ID_1, SPAN_ID_PREFIX)))

      When(s"kafka-streams topology is started and then stopped forcefully after few sec")
      var topology = new StreamTopology(kafkaConfig, spanBufferConfig)
      topology.start()
      Thread.sleep(10000)
      topology.close() shouldBe true

      Then(s"on restart of the topology, we should be able to read span-buffer object created in previous run from the '$OUTPUT_TOPIC' topic")
      topology = new StreamTopology(kafkaConfig, spanBufferConfig)
      topology.start()

      // produce one more span record with same traceId to trigger punctuate
      produceSpansAsync(1,
        produceInterval = 1.seconds,
        List(SpanDescription(TRACE_ID_1, SPAN_ID_PREFIX)),
        startTimestamp = SPAN_BUFFERING_WINDOW_MS)

      val records: util.List[KeyValue[String, SpanBuffer]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 1, MAX_WAIT_FOR_OUTPUT_MS)

      validate(records, MAX_CHILD_SPANS)

      topology.close() shouldBe true
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
