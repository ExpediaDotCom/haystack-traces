package com.expedia.www.haystack.span.stitcher.integration

import java.util.{List => JList}

import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.span.stitcher.StreamTopology
import com.expedia.www.haystack.span.stitcher.config.entities.{KafkaConfiguration, StitchConfiguration}
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
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
      val stitchConfig = StitchConfiguration(1000, PUNCTUATE_INTERVAL_MS, SPAN_STITCH_WINDOW_MS, loggingEnabled = false, 10000)
      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG), OUTPUT_TOPIC, INPUT_TOPIC, AutoOffsetReset.EARLIEST)

      When("spans are produced in 'input' topic async, and kafka-streams topology is started")
      produceSpanAsync(MAX_CHILD_SPANS, 2.seconds, List(TestSpanMetadata(TRACE_ID, SPAN_ID_PREFIX)))
      new StreamTopology(kafkaConfig, stitchConfig).start()

      Then("we should read one stitch span object from 'output' topic")
      val result: JList[KeyValue[String, StitchedSpan]] =
          IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 1, 12000)
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

