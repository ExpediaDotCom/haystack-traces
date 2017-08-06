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
 */package com.expedia.www.haystack.span.stitcher.integration

import java.util.{List => JList}

import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.span.stitcher.StreamTopology
import com.expedia.www.haystack.span.stitcher.config.entities.{KafkaConfiguration, StitchConfiguration}
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.{KeyValue, StreamsConfig}

import scala.collection.JavaConversions._
import scala.concurrent.duration._

class SpanStitchTopologySpec extends BaseIntegrationTestSpec {

  private val MAX_CHILD_SPANS = 5
  private val TRACE_ID = "unique-trace-id"
  private val SPAN_ID_PREFIX = "span-id"
  APP_ID = "haystack-topology-test"

  "Stitch Span Topology" should {
    "consume spans from input topic and stitch them together" in {
      Given("a set of spans with stitching and kafka specific configurations")
      val stitchConfig = StitchConfiguration(1000, PUNCTUATE_INTERVAL_MS, SPAN_STITCH_WINDOW_MS, loggingEnabled = false, 3000)
      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG), OUTPUT_TOPIC, INPUT_TOPIC, AutoOffsetReset.EARLIEST, new WallclockTimestampExtractor)

      When("spans are produced in 'input' topic async, and kafka-streams topology is started")
      produceSpansAsync(MAX_CHILD_SPANS, 2.seconds, List(SpanDescription(TRACE_ID, SPAN_ID_PREFIX)))
      new StreamTopology(kafkaConfig, stitchConfig).start()

      Then("we should read one stitch span object from 'output' topic")
      val result: JList[KeyValue[String, StitchedSpan]] =
          IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 1, 15000)
      validateStitchedSpan(result, MAX_CHILD_SPANS)
    }
  }

  // validate the stitched span object
  private def validateStitchedSpan(records: JList[KeyValue[String, StitchedSpan]], childSpanCount: Int) = {
    // expect only one stitched span object
    records.size() shouldBe 1
    validateChildSpans(records.head.value, TRACE_ID, SPAN_ID_PREFIX, MAX_CHILD_SPANS)
  }
}

