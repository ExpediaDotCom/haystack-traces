/*
 *  Copyright 2017 Expedia, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.expedia.www.haystack.stitched.span.collector.integration

import com.expedia.www.haystack.stitched.span.collector.writers.es.index.document.StitchedSpanIndex
import org.json4s.jackson.Serialization

import scala.collection.JavaConversions._

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
      verifyCassandraWrites()
    }
  }

  private def verifyCassandraWrites(): Unit = {
    val records = queryAllCassandra()
    records should have size TOTAL_STITCHED_SPANS
    records.foreach(rec => {
      (0 until TOTAL_STITCHED_SPANS).toSet should contain(rec.id.toInt)
      rec.stitchedSpan should not be null
      rec.stitchedSpan.getChildSpansCount shouldBe TOTAL_SPANS_PER_STITCHED_SPAN
      rec.stitchedSpan.getChildSpansList.zipWithIndex foreach {
        case (sp, idx) =>
          sp.getSpanId shouldBe s"${rec.id}_$idx"
          sp.getProcess.getServiceName shouldBe s"service$idx"
          sp.getOperationName shouldBe s"op$idx"
      }
    })
  }

  private def verifyElasticSearchWrites(): Unit = {
    val matchAllQuery =
     """{
       |    "query": {
       |        "match_all": {}
       |    }
       |}""".stripMargin

    var docs = queryElasticSearch(matchAllQuery)
    docs.size shouldBe TOTAL_STITCHED_SPANS
    for (doc <- docs;
         stitchedSpanIdx = Serialization.read[StitchedSpanIndex](doc)) {
      stitchedSpanIdx.duration shouldBe 0
      stitchedSpanIdx.spans should have size TOTAL_SPANS_PER_STITCHED_SPAN
    }

    val spanSpecificQuery =
      """
        |{
        |  "query": {
        |    "bool": {
        |      "must": [
        |        {
        |          "nested": {
        |            "path": "spans",
        |            "query": {
        |              "bool": {
        |                "must": [
        |                  {
        |                    "match": {
        |                      "spans.service": "service0"
        |                    }
        |                  },
        |                  {
        |                    "match": {
        |                      "spans.operation": "op0"
        |                    }
        |                  }
        |                ]
        |              }
        |            }
        |          }
        |        }
        |      ]
        |}}}
      """.stripMargin
    docs = queryElasticSearch(matchAllQuery)
    docs.size shouldBe TOTAL_STITCHED_SPANS

    val emptyResponseQuery =
      """
        |{
        |  "query": {
        |    "bool": {
        |      "must": [
        |        {
        |          "nested": {
        |            "path": "spans",
        |            "query": {
        |              "bool": {
        |                "must": [
        |                  {
        |                    "match": {
        |                      "spans.service": "service0"
        |                    }
        |                  },
        |                  {
        |                    "match": {
        |                      "spans.operation": "op1"
        |                    }
        |                  }
        |                ]
        |              }
        |            }
        |          }
        |        }
        |      ]
        |}}}
      """.stripMargin
    docs = queryElasticSearch(emptyResponseQuery)
    docs.size shouldBe 0

    val tagQuery =  """
                      |{
                      |  "query": {
                      |    "bool": {
                      |      "must": [
                      |        {
                      |          "nested": {
                      |            "path": "spans",
                      |            "query": {
                      |              "bool": {
                      |                "must": [
                      |                  {
                      |                    "match": {
                      |                      "spans.service": "service2"
                      |                    }
                      |                  },
                      |                  {
                      |                    "match": {
                      |                      "spans.operation": "op2"
                      |                    }
                      |                  },
                      |                  {
                      |                    "match": {
                      |                      "spans.tags.errorcode": "404"
                      |                    }
                      |                  }
                      |                ]
                      |              }
                      |            }
                      |          }
                      |        }
                      |      ]
                      |}}}
                    """.stripMargin
    docs = queryElasticSearch(tagQuery)
    docs.size shouldBe TOTAL_STITCHED_SPANS
  }
}
