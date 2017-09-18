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

package com.expedia.www.haystack.trace.indexer.integration

import java.util.UUID
import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import com.expedia.open.tracing.Tag.TagType
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.open.tracing.{Log, Process, Span, Tag}
import com.expedia.www.haystack.trace.indexer.config.entities.SpanAccumulatorConfiguration
import com.expedia.www.haystack.trace.indexer.integration.clients.{CassandraTestClient, ElasticSearchTestClient, KafkaTestClient}
import com.google.gson.JsonObject
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.scalatest._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class TraceDescription(traceId: String, spanIdPrefix: String)

abstract class BaseIntegrationTestSpec extends WordSpec with GivenWhenThen with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  protected val MAX_WAIT_FOR_OUTPUT_MS = 12000

  protected val spanAccumulatorConfig = SpanAccumulatorConfiguration(
    minTracesPerCache = 100,
    maxEntriesAllStores = 500,
    pollIntervalMillis = 2000L,
    bufferingWindowMillis = 6000L)

  protected var scheduler: ScheduledExecutorService = _

  val kafka = new KafkaTestClient
  val cassandra = new CassandraTestClient
  val elastic = new ElasticSearchTestClient

  override def beforeAll() {
    scheduler = Executors.newSingleThreadScheduledExecutor()
    kafka.prepare(getClass.getSimpleName)
    cassandra.prepare()
    elastic.prepare()
  }

  override def afterAll(): Unit = if(scheduler != null) scheduler.shutdownNow()

  protected def validateChildSpans(spanBuffer: SpanBuffer,
                                   traceId: String,
                                   spanIdPrefix: String,
                                   childSpanCount: Int): Unit = {
    spanBuffer.getTraceId shouldBe traceId

    spanBuffer.getChildSpansCount shouldBe childSpanCount

    (0 until spanBuffer.getChildSpansCount).toList foreach { idx =>
      spanBuffer.getChildSpans(idx).getSpanId shouldBe s"$spanIdPrefix-$idx"
      spanBuffer.getChildSpans(idx).getTraceId shouldBe spanBuffer.getTraceId
      spanBuffer.getChildSpans(idx).getProcess.getServiceName shouldBe s"service$idx"
      spanBuffer.getChildSpans(idx).getParentSpanId should not be null
      spanBuffer.getChildSpans(idx).getOperationName shouldBe s"op$idx"
    }
  }

  private def randomSpan(traceId: String, spanId: String, serviceName: String, operationName: String): Span = {
    val process = Process.newBuilder().setServiceName(serviceName)
    Span.newBuilder()
      .setTraceId(traceId)
      .setParentSpanId(UUID.randomUUID().toString)
      .setSpanId(spanId)
      .setProcess(process)
      .setOperationName(operationName)
      .setStartTime(System.currentTimeMillis())
      .addTags(Tag.newBuilder().setKey("errorcode").setType(TagType.LONG).setVLong(404))
      .addTags(Tag.newBuilder().setKey("role").setType(TagType.STRING).setVStr("haystack"))
      .addLogs(Log.newBuilder().addFields(Tag.newBuilder().setKey("exceptionType").setType(TagType.STRING).setVStr("external").build()).build())
      .build()
  }

  protected def produceSpansAsync(maxSpansPerTrace: Int,
                                  produceInterval: FiniteDuration,
                                  traceDescription: List[TraceDescription],
                                  startRecordTimestamp: Long,
                                  maxRecordTimestamp: Long,
                                  startSpanIdxFrom: Int = 0): ScheduledFuture[_] = {
    var timestamp = startRecordTimestamp
    var cnt = 0
    scheduler.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        if(cnt < maxSpansPerTrace) {
          val spans = traceDescription.map(sd => {
            new KeyValue[String, Span](sd.traceId, randomSpan(sd.traceId, s"${sd.spanIdPrefix}-${startSpanIdxFrom + cnt}", s"service${startSpanIdxFrom + cnt}", s"op${startSpanIdxFrom + cnt}"))
          }).asJava
          IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            kafka.INPUT_TOPIC,
            spans,
            kafka.TEST_PRODUCER_CONFIG,
            timestamp)
          timestamp = timestamp + (maxRecordTimestamp / (maxSpansPerTrace - 1))
        }
        cnt = cnt + 1
      }
    }, 0, produceInterval.toMillis, TimeUnit.MILLISECONDS)
  }

  def verifyCassandraWrites(traceDescriptions: Seq[TraceDescription], minSpansPerTrace: Int, maxSpansPerTrace: Int): Unit = {
    val rows = cassandra.queryAll()
    rows should have size traceDescriptions.size

    rows.foreach(row => {
      val descr = traceDescriptions.find(_.traceId == row.id).get
      row.spanBuffer should not be null
      row.spanBuffer.getChildSpansCount should be >=minSpansPerTrace
      row.spanBuffer.getChildSpansCount should be <=maxSpansPerTrace

      row.spanBuffer.getChildSpansList.zipWithIndex foreach {
        case (sp, idx) =>
          sp.getSpanId shouldBe s"${descr.spanIdPrefix}-$idx"
          sp.getProcess.getServiceName shouldBe s"service$idx"
          sp.getOperationName shouldBe s"op$idx"
      }
    })
  }

  def verifyElasticSearchWrites(traceIds: Seq[String]): Unit = {
    def extractTraceIdFromDocId(docId: String): String = {
      StringUtils.substring(docId, 0, StringUtils.lastIndexOf(docId, "_"))
    }

    val matchAllQuery =
      """{
        |    "query": {
        |        "match_all": {}
        |    }
        |}""".stripMargin

    var docs = elastic.query(matchAllQuery)
    docs.size shouldBe traceIds.size
    (0 until docs.size()).toList foreach { idx =>
      val docId = docs.get(idx).asInstanceOf[JsonObject].get("_id").getAsString
      val traceId = extractTraceIdFromDocId(docId)
      traceIds should contain(traceId)
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
    docs = elastic.query(spanSpecificQuery)
    docs.size shouldBe traceIds.size

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
    docs = elastic.query(emptyResponseQuery)
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
                      |                      "spans.errorcode": "404"
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
    docs = elastic.query(tagQuery)
    docs.size shouldBe traceIds.size
  }
}
