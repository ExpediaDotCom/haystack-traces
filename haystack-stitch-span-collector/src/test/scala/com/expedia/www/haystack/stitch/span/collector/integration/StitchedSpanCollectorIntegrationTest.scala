package com.expedia.www.haystack.stitch.span.collector.integration

import com.expedia.www.haystack.stitch.span.collector.writers.es.index.generator.Document.IndexDataModel
import org.json4s.jackson.Serialization
import scala.collection.JavaConversions._

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
      verifyCassandraWrites()
    }
  }

  private def verifyCassandraWrites(): Unit = {
    val records = queryAllCassandra()
    records should have size 10
    records.foreach(rec => {
      (0 until 10).toSet should contain(rec.id.toInt)
      rec.stitchedSpan should not be null
      rec.stitchedSpan.getChildSpansCount shouldBe 3
      rec.stitchedSpan.getChildSpansList.zipWithIndex foreach {
        case (sp, idx) =>
          sp.getSpanId shouldBe s"${rec.id}_$idx"
          sp.getProcess.getServiceName shouldBe s"service-$idx"
          sp.getOperationName shouldBe s"op-$idx"
      }
    })
  }

  private def verifyElasticSearchWrites(): Unit = {
    val docs = queryElasticSearch(matchAllQuery)
    docs.size shouldBe TOTAL_STITCHED_SPANS
    for (doc <- docs;
         indexMap = Serialization.read[IndexDataModel](doc)) {
      (0 until 3).toList foreach { idx =>
        val serviceName = s"service-$idx"
        indexMap should contain key serviceName
        indexMap(serviceName) should contain key "_all"
        indexMap(serviceName) should contain key s"op-$idx"
      }
    }
  }
}
