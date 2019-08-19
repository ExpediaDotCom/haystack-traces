package com.expedia.www.haystack.trace.indexer.integration

import java.util

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.indexer.StreamRunner
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils

import scala.concurrent.duration._

class ShowValuesIndexingTopologySpec extends BaseIntegrationTestSpec {
  private val MAX_CHILD_SPANS_PER_TRACE = 5
  private val TRACE_ID_6 = "traceid-6"
  private val TRACE_ID_7 = "traceid-7"
  private val SPAN_ID_PREFIX_1 = TRACE_ID_6 + "span-id-"
  private val SPAN_ID_PREFIX_2 = TRACE_ID_7 + "span-id-"

  "Show values topology" should {
    s"consume spans from input ${kafka.INPUT_TOPIC} and write ShowValueDocs for the whitelisted fields for which showValues is true" in {
      Given("a set of spans with different servicenames and a project configuration")
      val kafkaConfig = kafka.buildConfig
      val esConfig = elastic.buildConfig
      val indexTagsConfig = elastic.indexingConfig
      val backendConfig = traceBackendClient.buildConfig
      val serviceMetadataConfig = elastic.buildServiceMetadataConfig
      val showValuesConfig = elastic.buildShowValuesConfig

      When(s"spans are produced in '${kafka.INPUT_TOPIC}' topic async, and kafka-streams topology is started")
      val traceDescriptions = List(TraceDescription(TRACE_ID_6, SPAN_ID_PREFIX_1), TraceDescription(TRACE_ID_7, SPAN_ID_PREFIX_2))

      produceSpansAsync(MAX_CHILD_SPANS_PER_TRACE,
        1.seconds,
        traceDescriptions,
        0,
        spanAccumulatorConfig.bufferingWindowMillis)

      val topology = new StreamRunner(kafkaConfig, spanAccumulatorConfig, esConfig, backendConfig, serviceMetadataConfig, showValuesConfig, indexTagsConfig)
      topology.start()

      Then(s"we should read two multiple service operation combinations in elastic search")
      try {
        val result: util.List[KeyValue[String, SpanBuffer]] =
          IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(kafka.RESULT_CONSUMER_CONFIG, kafka.OUTPUT_TOPIC, 2, MAX_WAIT_FOR_OUTPUT_MS)
        Thread.sleep(6000)
        verifyFields()
      } finally {
        topology.close()
      }
    }
  }
}
