package com.expedia.www.haystack.span.stitcher.integration

import java.util
import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.span.stitcher.StreamTopology
import com.expedia.www.haystack.span.stitcher.config.entities.{ChangelogConfiguration, KafkaConfiguration, StitchConfiguration}
import org.apache.kafka.streams.{KeyValue, StreamsConfig}
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset

import scala.collection.JavaConversions._
import scala.concurrent.duration._

class EvictedStitchedSpanSpec extends BaseIntegrationTestSpec {
  private val MAX_CHILD_SPANS = 5
  private val TRACE_ID_1 = "traceid-1"
  private val TRACE_ID_2 = "traceid-2"
  private val SPAN_ID_PREFIX = "span-id-"

  "Stitch Span Topology" should {
    s"consume spans from input '$INPUT_TOPIC' and stitch them together" in {
      Given("a set of spans produced async with extremely small store size configuration")
      val stitchConfig = StitchConfiguration(
        initialStoreSize = 1,
        maxEntriesAllStores = 2,
        PUNCTUATE_INTERVAL_MS,
        SPAN_STITCH_WINDOW_MS,
        AUTO_COMMIT_INTERVAL_MS)
      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG),
        OUTPUT_TOPIC,
        INPUT_TOPIC,
        AutoOffsetReset.EARLIEST,
        new FailOnInvalidTimestamp,
        ChangelogConfiguration(enabled = true))
      produceSpansAsync(MAX_CHILD_SPANS,
        produceInterval = 1.seconds,
        List(SpanDescription(TRACE_ID_1, SPAN_ID_PREFIX), SpanDescription(TRACE_ID_2, SPAN_ID_PREFIX)))

      When(s"kafka-streams topology is started")
      val topology = new StreamTopology(kafkaConfig, stitchConfig)
      topology.start()

      Then(s"we should get multiple stitch-span objects bearing only 1 span due to early eviction from store")
      val records: util.List[KeyValue[String, StitchedSpan]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 10, MAX_WAIT_FOR_OUTPUT_MS)

      validateStitchedSpan(records)
      topology.close() shouldBe true
    }
  }

  // validate the received records
  private def validateStitchedSpan(records: util.List[KeyValue[String, StitchedSpan]]) = {
    records.map(_.key).toSet should contain allOf (TRACE_ID_1, TRACE_ID_2)
    records.foreach(rec => rec.value.getChildSpansCount shouldBe 1)
  }
}
