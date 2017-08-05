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
package com.expedia.www.haystack.span.stitcher.integration

import java.util.{List => JList}

import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.span.stitcher.StreamTopology
import com.expedia.www.haystack.span.stitcher.config.entities.{KafkaConfiguration, StitchConfiguration}
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
import org.apache.kafka.streams.{KeyValue, StreamsConfig}

import scala.concurrent.duration._
import scala.collection.JavaConversions._

class SpanStitchWithMultiTraceIdsTopology extends BaseIntegrationTestSpec {
  private val MAX_CHILD_SPANS = 5
  private val TRACE_ID_1 = "traceid-1"
  private val TRACE_ID_2 = "traceid-2"
  private val SPAN_ID_PREFIX_1 = TRACE_ID_1 + "span-id-"
  private val SPAN_ID_PREFIX_2 = TRACE_ID_2 + "span-id-"

  APP_ID = "multi-trace-topology-test"

  "Stitch Span Topology" should {
    "consume spans from input topic and stitch them together" in {
      Given("a set of spans with two different traceIds and stitching/kafka specific configurations")
      val stitchConfig = StitchConfiguration(1000, PUNCTUATE_INTERVAL_MS, SPAN_STITCH_WINDOW_MS, loggingEnabled = true, 3000)
      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG), OUTPUT_TOPIC, INPUT_TOPIC, AutoOffsetReset.EARLIEST)

      When("spans are produced in 'input' topic async, and kafka-streams topology is started")
      produceSpansAsync(MAX_CHILD_SPANS,
        1750.millis,
        List(SpanDescription(TRACE_ID_1, SPAN_ID_PREFIX_1), SpanDescription(TRACE_ID_2, SPAN_ID_PREFIX_2)))
      val topology = new StreamTopology(kafkaConfig, stitchConfig)
      topology.start()

      Then("we should read two stitch span objects with differet traceIds from 'output' topic")
      val result: JList[KeyValue[String, StitchedSpan]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 2, 12000)

      validateStitchedSpan(result, MAX_CHILD_SPANS)

      topology.close() shouldBe true
    }
  }

  // validate the received records
  private def validateStitchedSpan(records: JList[KeyValue[String, StitchedSpan]], childSpanCount: Int) = {
    // expect only one stitched span object
    records.size() shouldBe 2

    // both traceIds should be present as different stitched span objects
    records.map(_.key) should contain allOf (TRACE_ID_1, TRACE_ID_2)

    records.foreach(record => {
      record.key match {
        case TRACE_ID_1 => validateChildSpans(record.value, TRACE_ID_1, SPAN_ID_PREFIX_1, MAX_CHILD_SPANS)
        case TRACE_ID_2 => validateChildSpans(record.value, TRACE_ID_2, SPAN_ID_PREFIX_2, MAX_CHILD_SPANS)
      }
    })
  }
}
