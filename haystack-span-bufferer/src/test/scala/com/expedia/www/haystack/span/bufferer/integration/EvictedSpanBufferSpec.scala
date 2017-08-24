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

class EvictedSpanBufferSpec extends BaseIntegrationTestSpec {
  private val MAX_CHILD_SPANS = 5
  private val TRACE_ID_1 = "traceid-1"
  private val TRACE_ID_2 = "traceid-2"
  private val SPAN_ID_PREFIX = "span-id-"

  "Span Buffer Topology" should {
    s"consume spans from input '$INPUT_TOPIC' and buffer them together for a given traceId" in {
      Given("a set of spans produced async with extremely small store size configuration")
      val spanBufferConfig = SpanBufferConfiguration(
        initialStoreSize = 1,
        maxEntriesAllStores = 2,
        PUNCTUATE_INTERVAL_MS,
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
        List(SpanDescription(TRACE_ID_1, SPAN_ID_PREFIX), SpanDescription(TRACE_ID_2, SPAN_ID_PREFIX)))

      When(s"kafka-streams topology is started")
      val topology = new StreamTopology(kafkaConfig, spanBufferConfig)
      topology.start()

      Then(s"we should get multiple span-buffers bearing only 1 span due to early eviction from store")
      val records: util.List[KeyValue[String, SpanBuffer]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 10, MAX_WAIT_FOR_OUTPUT_MS)

      validate(records)
      topology.close() shouldBe true
    }
  }

  // validate the received records
  private def validate(records: util.List[KeyValue[String, SpanBuffer]]) = {
    records.map(_.key).toSet should contain allOf (TRACE_ID_1, TRACE_ID_2)
    records.foreach(rec => rec.value.getChildSpansCount shouldBe 1)
  }
}
