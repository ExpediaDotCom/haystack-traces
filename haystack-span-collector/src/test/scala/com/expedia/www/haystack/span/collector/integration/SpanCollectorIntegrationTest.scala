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

package com.expedia.www.haystack.span.collector.integration

import com.google.gson.JsonObject

import scala.collection.JavaConversions._

class SpanCollectorIntegrationTest extends BaseIntegrationTestSpec {
  private val TOTAL_SPAN_BUFFERS = 10
  private val TOTAL_SPANS_PER_SPAN_BUFFER = 3
  private val SPAN_DURATION = 1000

  "Span collector" should {
    s"read span buffer proto objects from kafka topic '$CONSUMER_TOPIC' and write to es/casandra" in {
      Given("a list of span buffers")
      val st = createSpanBuffers(TOTAL_SPAN_BUFFERS, TOTAL_SPANS_PER_SPAN_BUFFER, SPAN_DURATION)
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
    records should have size TOTAL_SPAN_BUFFERS
    records.foreach(rec => {
      (0 until TOTAL_SPAN_BUFFERS).toSet should contain(rec.id.toInt)
      rec.spanBuffer should not be null
      rec.spanBuffer.getChildSpansCount shouldBe TOTAL_SPANS_PER_SPAN_BUFFER
      rec.spanBuffer.getChildSpansList.zipWithIndex foreach {
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
    docs.size shouldBe TOTAL_SPAN_BUFFERS
    (0 until docs.size()).toList foreach { idx =>
      docs.get(idx).asInstanceOf[JsonObject].get("_id").getAsString should startWith(idx.toString + "_")
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
    docs = queryElasticSearch(spanSpecificQuery)
    docs.size shouldBe TOTAL_SPAN_BUFFERS
    (0 until docs.size()).toList foreach { idx =>
      docs.get(idx).asInstanceOf[JsonObject].get("_id").getAsString should startWith(idx.toString + "_")
    }

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
    docs.size shouldBe TOTAL_SPAN_BUFFERS
  }
}
