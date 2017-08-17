package com.expedia.www.haystack.stitch.span.collector.integration

class StitchedSpanCollectorIntegrationTest extends BaseIntegrationTestSpec {
  private val TOTAL_STITCHED_SPANS = 10
  private val TOTAL_SPANS_PER_STITCHED_SPAN = 3
  private val SPAN_DURATION = 1000

  "StitchedSpan collector" should {
    s"read stitched spans from kafka topic '$CONSUMER_TOPIC' and write to es/casandra" in {
      Given("a list of stitched spans")
      val st = createStitchedSpans(TOTAL_STITCHED_SPANS, TOTAL_SPANS_PER_STITCHED_SPAN, SPAN_DURATION)
      When(s"they are produced to kafka topic '$CONSUMER_TOPIC")
      produceToKafka(st)
      Then("indexing data should be written to elastic search and full trace is stored in cassandra")
      Thread.sleep(5000)
      verifyElasticSearchWrites()
    }
  }

  private def verifyElasticSearchWrites(): Unit = {
    (0 until 3).toList foreach { idx =>
      var docs = queryElasticSearch("{\"query\": {\"match\": {\"spans.operation\": \"op-" + idx.toString + "\"}}}")
      docs.size shouldBe TOTAL_STITCHED_SPANS

      docs = queryElasticSearch("{\"query\": {\"match\": {\"spans.service\": \"service-" + idx.toString + "\"}}}")
      docs.size shouldBe TOTAL_STITCHED_SPANS
    }
    val docs = queryElasticSearch("{\"query\": {\"match\": {\"spans.duration\": " + SPAN_DURATION + "}}}")
    docs.size shouldBe TOTAL_STITCHED_SPANS
  }
}
