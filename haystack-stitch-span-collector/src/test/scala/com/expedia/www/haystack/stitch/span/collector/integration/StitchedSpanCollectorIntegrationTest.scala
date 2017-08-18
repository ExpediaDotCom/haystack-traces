package com.expedia.www.haystack.stitch.span.collector.integration

import com.expedia.www.haystack.stitch.span.collector.writers.es.index.generator.Document.IndexDataModel
import org.json4s.jackson.Serialization

class StitchedSpanCollectorIntegrationTest extends BaseIntegrationTestSpec {
  private val TOTAL_STITCHED_SPANS = 10
  private val TOTAL_SPANS_PER_STITCHED_SPAN = 3
  private val SPAN_DURATION = 1000
  private val matchAllQuery = "{\"query\":{\"match_all\":{\"boost\":1.0}}}"

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
    val docs = queryElasticSearch(matchAllQuery)
    docs.size shouldBe TOTAL_STITCHED_SPANS
    for (elem <- docs) {
      val data = Serialization.read[IndexDataModel](elem)
      data should contain key "service-2"
      data should contain key "service-1"
      data should contain key "service-0"
    }
  }
}
