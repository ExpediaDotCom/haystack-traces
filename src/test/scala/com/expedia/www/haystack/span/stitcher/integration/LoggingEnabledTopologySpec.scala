package com.expedia.www.haystack.span.stitcher.integration

import java.util.{List => JList}

import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.span.stitcher.StreamTopology
import com.expedia.www.haystack.span.stitcher.config.entities.{ChangelogConfiguration, KafkaConfiguration, StitchConfiguration}
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
import org.apache.kafka.streams.{KeyValue, StreamsConfig}

import scala.collection.JavaConversions._
import scala.concurrent.duration._

class LoggingEnabledTopologySpec extends BaseIntegrationTestSpec {

  private val MAX_CHILD_SPANS = 5
  private val TRACE_ID_1 = "traceid-1"
  private val SPAN_ID_PREFIX = "span-id-"

  "Stitch Span Topology" should {
    s"consume spans from input '$INPUT_TOPIC' and stitch them together" in {
      Given("a set of spans with two different traceIds and stitching+kafka configurations")
      val stitchConfig = StitchConfiguration(
        MAX_STITCHED_RECORDS_IN_MEM,
        PUNCTUATE_INTERVAL_MS,
        SPAN_STITCH_WINDOW_MS,
        AUTO_COMMIT_INTERVAL_MS)
      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG),
        OUTPUT_TOPIC,
        INPUT_TOPIC,
        AutoOffsetReset.EARLIEST,
        new FailOnInvalidTimestamp,
        ChangelogConfiguration(enabled = true))
      When(s"spans are produced in '$INPUT_TOPIC' topic async, and kafka-streams topology is started")
      produceSpansAsync(MAX_CHILD_SPANS,
        produceInterval = 2.seconds,
        List(SpanDescription(TRACE_ID_1, SPAN_ID_PREFIX)))
      val topology = new StreamTopology(kafkaConfig, stitchConfig)
      topology.start()

      Then(s"we should read one stitch span objects from '$OUTPUT_TOPIC' topic")
      val result: JList[KeyValue[String, StitchedSpan]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 1, MAX_WAIT_FOR_OUTPUT_MS)

      validateStitchedSpan(result, MAX_CHILD_SPANS)

      // validate if the changelog appear in the changelog topic
      validateChangelogs()
      topology.close() shouldBe true
    }
  }

  private def validateChangelogs(): Unit = {
    IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
      CHANGELOG_CONSUMER_CONFIG,
      CHANGELOG_TOPIC,
      2,
      MAX_WAIT_FOR_OUTPUT_MS)
  }

  // validate the received records
  private def validateStitchedSpan(records: JList[KeyValue[String, StitchedSpan]], childSpanCount: Int) = {
    // expect only one stitched span object
    records.size() shouldBe 1

    // both traceIds should be present as different stitched span objects
    records.head.key shouldBe TRACE_ID_1

    validateChildSpans(records.head.value, TRACE_ID_1, SPAN_ID_PREFIX, MAX_CHILD_SPANS)
  }
}
