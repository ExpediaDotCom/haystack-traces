package com.expedia.www.haystack.span.stitcher.integration

import java.util.{List => JList}

import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.span.stitcher.StreamTopology
import com.expedia.www.haystack.span.stitcher.config.entities.{ChangelogConfiguration, KafkaConfiguration, StitchConfiguration}
import org.apache.kafka.streams.{KeyValue, StreamsConfig}
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
import scala.concurrent.duration._
import scala.collection.JavaConversions._

class FailedTopologyRecoverySpec extends BaseIntegrationTestSpec {
  private val MAX_CHILD_SPANS = 5
  private val TRACE_ID_1 = "traceid-1"
  private val SPAN_ID_PREFIX = "span-id-"

  "Stitch Span Topology" should {
    s"consume spans from input '$INPUT_TOPIC' and stitch them together" in {
      Given("a set of spans produced async with stitching+kafka configurations")
      val stitchConfig = StitchConfiguration(
        MAX_STITCHED_RECORDS_IN_MEM,
        PUNCTUATE_INTERVAL_MS,
        1 << 10,
        AUTO_COMMIT_INTERVAL_MS)
      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG),
        OUTPUT_TOPIC,
        INPUT_TOPIC,
        AutoOffsetReset.EARLIEST,
        new FailOnInvalidTimestamp,
        ChangelogConfiguration(enabled = true))
      produceSpansAsync(MAX_CHILD_SPANS,
        produceInterval = 2.seconds,
        List(SpanDescription(TRACE_ID_1, SPAN_ID_PREFIX)))

      When(s"kafka-streams topology is started and then stopped forcefully after few sec")
      var topology = new StreamTopology(kafkaConfig, stitchConfig)
      topology.start()
      Thread.sleep(12000)
      topology.close() shouldBe true

      Then(s"on restart of the topology, we should be able to read stitch span object created in previous run from the '$OUTPUT_TOPIC' topic")
      topology = new StreamTopology(kafkaConfig, stitchConfig.copy(stitchWindowMillis = SPAN_STITCH_WINDOW_MS))
      topology.start()

      // produce one more span record with same traceId to trigger punctuate
      produceSpansAsync(1,
        produceInterval = 1.seconds,
        List(SpanDescription(TRACE_ID_1, SPAN_ID_PREFIX)),
        startTimestamp = PUNCTUATE_INTERVAL_MS + 100)

      val records: JList[KeyValue[String, StitchedSpan]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 1, MAX_WAIT_FOR_OUTPUT_MS)

      validateStitchedSpan(records, MAX_CHILD_SPANS + 1) // 1 extra span is created in the rerun

      topology.close() shouldBe true
    }
  }

  // validate the received records
  private def validateStitchedSpan(records: JList[KeyValue[String, StitchedSpan]], childSpanCount: Int) = {
    // expect only one stitched span object
    records.size() shouldBe 1
    records.head.key shouldBe TRACE_ID_1
    validateChildSpans(records.head.value, TRACE_ID_1, SPAN_ID_PREFIX, MAX_CHILD_SPANS)
  }
}
