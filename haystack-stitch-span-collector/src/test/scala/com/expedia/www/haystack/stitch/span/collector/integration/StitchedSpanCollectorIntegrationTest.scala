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
    val docs = queryElasticSearch("{\"query\": {\"match\": {\"service-1.op-1\": \"service-1\"}}}")
    docs.size shouldBe TOTAL_STITCHED_SPANS
  }
}
