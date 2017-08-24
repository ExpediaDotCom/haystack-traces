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

class SpanBufferingWithMultiTraceIdsTopology extends BaseIntegrationTestSpec {
  private val MAX_CHILD_SPANS = 5
  private val TRACE_ID_1 = "traceid-1"
  private val TRACE_ID_2 = "traceid-2"
  private val SPAN_ID_PREFIX_1 = TRACE_ID_1 + "span-id-"
  private val SPAN_ID_PREFIX_2 = TRACE_ID_2 + "span-id-"

  "Span Buffering Topology" should {
    s"consume spans from input '$INPUT_TOPIC' and buffer them together for every unique traceId" in {
      Given("a set of spans with two different traceIds and spanBuffering and kafka specific configurations")
      val spanBufferConfig = SpanBufferConfiguration(
        INITIAL_STORE_CAPACITY,
        MAX_SPAN_BUFFERS_IN_MEM,
        PUNCTUATE_INTERVAL_MS,
        SPAN_BUFFERING_WINDOW_MS,
        AUTO_COMMIT_INTERVAL_MS)
      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG),
        OUTPUT_TOPIC,
        INPUT_TOPIC,
        AutoOffsetReset.EARLIEST,
        new FailOnInvalidTimestamp,
        ChangelogConfiguration(enabled = false))

      When(s"spans are produced in '$INPUT_TOPIC' topic async, and kafka-streams topology is started")
      produceSpansAsync(MAX_CHILD_SPANS,
        1.seconds,
        List(SpanDescription(TRACE_ID_1, SPAN_ID_PREFIX_1), SpanDescription(TRACE_ID_2, SPAN_ID_PREFIX_2)))
      val topology = new StreamTopology(kafkaConfig, spanBufferConfig)
      topology.start()

      Then(s"we should read two span buffers with different traceIds from '$OUTPUT_TOPIC' topic")
      val result: util.List[KeyValue[String, SpanBuffer]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 2, MAX_WAIT_FOR_OUTPUT_MS)

      validate(result, MAX_CHILD_SPANS)

      topology.close() shouldBe true
    }
  }

  // validate the received records
  private def validate(records: util.List[KeyValue[String, SpanBuffer]], childSpanCount: Int) = {
    records.size() shouldBe 2

    // both traceIds should be present as different span buffer objects
    records.map(_.key) should contain allOf (TRACE_ID_1, TRACE_ID_2)

    records.foreach(record => {
      record.key match {
        case TRACE_ID_1 => validateChildSpans(record.value, TRACE_ID_1, SPAN_ID_PREFIX_1, MAX_CHILD_SPANS)
        case TRACE_ID_2 => validateChildSpans(record.value, TRACE_ID_2, SPAN_ID_PREFIX_2, MAX_CHILD_SPANS)
      }
    })
  }
}
