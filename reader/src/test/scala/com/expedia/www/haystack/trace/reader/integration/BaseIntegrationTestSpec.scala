/*
 *  Copyright 2017 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.expedia.www.haystack.trace.reader.integration

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.concurrent.Executors
import java.util.{Date, UUID}

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Cluster, ResultSet, Session, SimpleStatement}
import com.expedia.open.tracing.{Log, Span}
import com.expedia.open.tracing.api.TraceReaderGrpc
import com.expedia.open.tracing.api.TraceReaderGrpc.TraceReaderBlockingStub
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraTableSchema
import com.expedia.www.haystack.trace.commons.clients.es.document.TraceIndexDoc
import com.expedia.www.haystack.trace.commons.config.entities.{WhiteListIndexFields, WhitelistIndexField}
import com.expedia.www.haystack.trace.reader.Service
import com.expedia.www.haystack.trace.reader.unit.readers.builders.ValidTraceBuilder
import io.grpc.ManagedChannelBuilder
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.Index
import io.searchbox.indices.CreateIndex
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.scalatest._

import collection.JavaConverters._
import scala.collection.mutable

trait BaseIntegrationTestSpec extends FunSpec with GivenWhenThen with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with ValidTraceBuilder {
  protected implicit val formats = DefaultFormats
  protected var client: TraceReaderBlockingStub = _

  private val CASSANDRA_ENDPOINT = "cassandra"
  private val CASSANDRA_KEYSPACE = "haystack"
  private val CASSANDRA_TABLE = "traces"

  private val ELASTIC_SEARCH_ENDPOINT = "http://elasticsearch:9200"
  private val ELASTIC_SEARCH_WHITELIST_INDEX = "reload-configs"
  private val ELASTIC_SEARCH_WHITELIST_TYPE = "whitelist-index-fields"
  private val SPANS_INDEX_TYPE = "spans"

  private val executors = Executors.newSingleThreadExecutor()

  private val HAYSTACK_TRACES_INDEX = {
    val date = new Date()

    val dateBucket = new SimpleDateFormat("yyyy-MM-dd").format(date)
    val hourBucket = new SimpleDateFormat("HH").format(date).toInt / 6

    s"haystack-traces-$dateBucket-$hourBucket"
  }
  private val INDEX_TEMPLATE = """{
                                 |    "template": "haystack-traces*",
                                 |    "settings": {
                                 |        "number_of_shards": 1,
                                 |        "index.mapping.ignore_malformed": true,
                                 |        "analysis": {
                                 |            "normalizer": {
                                 |                "lowercase_normalizer": {
                                 |                    "type": "custom",
                                 |                    "filter": ["lowercase"]
                                 |                }
                                 |            }
                                 |        }
                                 |    },
                                 |    "aliases": {
                                 |        "haystack-traces": {}
                                 |    },
                                 |    "mappings": {
                                 |        "spans": {
                                 |            "_all": {
                                 |                "enabled": false
                                 |            },
                                 |            "_source": {
                                 |                "includes": ["traceid"]
                                 |            },
                                 |            "properties": {
                                 |                "traceid": {
                                 |                    "enabled": false
                                 |                },
                                 |                "spans": {
                                 |                    "type": "nested",
                                 |                    "properties": {
                                 |                        "servicename": {
                                 |                            "type": "keyword",
                                 |                            "normalizer": "lowercase_normalizer",
                                 |                            "doc_values": true,
                                 |                            "norms": false
                                 |                        },
                                 |                        "operationname": {
                                 |                            "type": "keyword",
                                 |                            "normalizer": "lowercase_normalizer",
                                 |                            "doc_values": true,
                                 |                            "norms": false
                                 |                        },
                                 |                        "starttime": {
                                 |                            "type": "long",
                                 |                            "doc_values": true
                                 |                        }
                                 |                    }
                                 |                }
                                 |            },
                                 |            "dynamic_templates": [{
                                 |                "strings_as_keywords_1": {
                                 |                    "match_mapping_type": "string",
                                 |                    "mapping": {
                                 |                        "type": "keyword",
                                 |                        "normalizer": "lowercase_normalizer",
                                 |                        "doc_values": false,
                                 |                        "norms": false
                                 |                    }
                                 |                }
                                 |            }, {
                                 |                "longs_disable_doc_norms": {
                                 |                    "match_mapping_type": "long",
                                 |                    "mapping": {
                                 |                        "type": "long",
                                 |                        "doc_values": false,
                                 |                        "norms": false
                                 |                    }
                                 |                }
                                 |            }]
                                 |        }
                                 |    }
                                 |}
                                 |""".stripMargin


  private var cassandraSession: Session = _
  private var esClient: JestClient = _

  override def beforeAll() {
    // setup cassandra
    cassandraSession = Cluster
      .builder()
      .addContactPoints(CASSANDRA_ENDPOINT)
      .build()
      .connect(CASSANDRA_KEYSPACE)
    deleteCassandraTableRows()

    // setup elasticsearch
    val factory = new JestClientFactory()
    factory.setHttpClientConfig(
      new HttpClientConfig.Builder(ELASTIC_SEARCH_ENDPOINT)
        .multiThreaded(true)
        .build())
    esClient = factory.getObject
    esClient.execute(new CreateIndex.Builder(HAYSTACK_TRACES_INDEX)
      .settings(INDEX_TEMPLATE)
      .build)

    executors.submit(new Runnable {
      override def run(): Unit = Service.main(null)
    })

    Thread.sleep(5000)

    client = TraceReaderGrpc.newBlockingStub(ManagedChannelBuilder.forAddress("localhost", 8088)
      .usePlaintext(true)
      .build())
  }

  private def deleteCassandraTableRows(): Unit = {
    cassandraSession.execute(new SimpleStatement(s"TRUNCATE $CASSANDRA_TABLE"))
  }

  protected def putTraceInCassandraAndEs(traceId: String = UUID.randomUUID().toString,
                                         spanId: String = UUID.randomUUID().toString,
                                         serviceName: String = "",
                                         operationName: String = "",
                                         tags: Map[String, String] = Map.empty): Unit = {
    insertTraceInCassandra(traceId, spanId, serviceName, operationName, tags)
    insertTraceInEs(traceId, spanId, serviceName, operationName, tags)
  }

  private def insertTraceInEs(traceId: String,
                              spanId: String,
                              serviceName: String,
                              operationName: String,
                              tags: Map[String, String]) = {
    import TraceIndexDoc._
    // create map using service, operation and tags
    val fieldMap:mutable.Map[String, Any] = mutable.Map(
      SERVICE_KEY_NAME -> serviceName,
      OPERATION_KEY_NAME -> operationName,
      START_TIME_KEY_NAME -> mutable.Set[Any](System.currentTimeMillis() * 1000)
    )
    tags.foreach(pair => fieldMap.put(pair._1.toLowerCase(), pair._2))

    // index the document
    val result = esClient.execute(new Index.Builder(TraceIndexDoc(traceId, 0, Seq(fieldMap)).json)
      .index(HAYSTACK_TRACES_INDEX)
      .`type`(SPANS_INDEX_TYPE)
      .build)

    if(result.getErrorMessage != null) {
      fail("Fail to execute the indexing request " + result.getErrorMessage)
    }
    // wait for few sec to let ES refresh its index
    Thread.sleep(5000)
  }

  protected def putWhitelistIndexFieldsInEs(fields: List[String]): Unit = {
    val whitelistFields = for(field <- fields) yield WhitelistIndexField(field, "string")
    esClient.execute(new Index.Builder(Serialization.write(WhiteListIndexFields(whitelistFields)))
      .index(ELASTIC_SEARCH_WHITELIST_INDEX)
      .`type`(ELASTIC_SEARCH_WHITELIST_TYPE)
      .build)

    // wait for few sec to let ES refresh its index and app to reload its config
    Thread.sleep(10000)
  }

  private def insertTraceInCassandra(traceId: String,
                                     spanId: String,
                                     serviceName: String,
                                     operationName: String,
                                     tags: Map[String, String]): ResultSet = {
    val spanBuffer = createSpanBufferWithSingleSpan(traceId, spanId, serviceName, operationName, tags)
    writeToCassandra(spanBuffer, traceId)
  }

  /*protected def insertTracesInEsForTimeline(serviceName: String,
                                            operationName: String,
                                            tags: Map[String, String],
                                            startTimeInMicroSec: Long,
                                            endTimeInMicroSec: Long,
                                            intervalInMicroSec: Int,
                                            spansInEachInterval: Int) = {
    val bucketCount = getBucketCount(startTimeInMicroSec, endTimeInMicroSec, intervalInMicroSec);
    System.out.println("------- insertTracesInEsForTimeline: bucket count ----------" + bucketCount)
    for (i <- 0 until bucketCount - 1) {
      val spanStartTimeInMicroSec = startTimeInMicroSec + (intervalInMicroSec * i)
      val fieldMap = createESFieldMap(serviceName, operationName, spanStartTimeInMicroSec, tags)
      for (j <- 0 until spansInEachInterval) {
        System.out.println("------- insertTracesInEsForTimeline: inserting span ----------" + i + "-------"+j)
        // index the document
        val result = esClient.execute(new Index.Builder(TraceIndexDoc(UUID.randomUUID().toString, 0, Seq(fieldMap)).json)
          .index(HAYSTACK_TRACES_INDEX)
          .`type`(SPANS_INDEX_TYPE)
          .build)
        if (result.getErrorMessage != null) {
          fail("Failed to execute the indexing request for insertTracesInEsForTimeline with message"
            + result.getErrorMessage)
        }
      }
    }
    // wait for few sec to let ES refresh its index
    Thread.sleep(5000)
  }

  protected def getBucketCount(startTime: Long, endTime: Long, interval: Int): Int = {
    val completeBuckets = ((endTime - startTime) / interval).toInt
    val partialBucket = if ((endTime - startTime) % interval > 0) 1 else 0
    completeBuckets + partialBucket
  }
  */

  protected def putTracesForTimeline(serviceName: String,
                                     operationName: String,
                                     starttimes: Seq[String]) = {
    starttimes.map(
      starttime => {
        System.out.println("------- putTracesForTimeline: starttime ----------" + starttime)
        val traceId = UUID.randomUUID().toString
        val spanId = UUID.randomUUID().toString

        insertTraceInCassandra(traceId, spanId, serviceName, operationName, Map.empty)
        System.out.println("------- putTracesForTimeline: inserted in cassandra ----------" + starttime)

        val fieldMap: mutable.Map[String, Any] = mutable.Map(
          SERVICE_KEY_NAME -> serviceName,
          OPERATION_KEY_NAME -> operationName,
          START_TIME_KEY_NAME -> mutable.Set[Any](starttime)
        )

        val result = esClient.execute(new Index.Builder(TraceIndexDoc(traceId, 0, Seq(fieldMap)).json)
          .index(HAYSTACK_TRACES_INDEX)
          .`type`(SPANS_INDEX_TYPE)
          .build)
        System.out.println("------- putTracesForTimeline: inserted in ES ----------" + starttime)

        if (result.getErrorMessage != null) {
          fail("Failed to execute the indexing request " + result.getErrorMessage)
        }
      }
    )
    // wait for few sec to let ES refresh its index
    Thread.sleep(5000)
  }

  // create map using service, operation, startTime and tags
  private def createESFieldMap(serviceName: String,
                               operationName: String,
                               spanStartTimeInMicroSec: Long,
                               tags: Map[String, String]): mutable.Map[String, Any] = {
    import TraceIndexDoc._
    val fieldMap: mutable.Map[String, Any] = mutable.Map(
      SERVICE_KEY_NAME -> serviceName,
      OPERATION_KEY_NAME -> operationName,
      START_TIME_KEY_NAME -> mutable.Set[Any](spanStartTimeInMicroSec)
    )
    tags.foreach(pair => fieldMap.put(pair._1.toLowerCase(), pair._2))

    fieldMap
  }

  private def writeToCassandra(spanBuffer: SpanBuffer, traceId: String) = {
    import CassandraTableSchema._

    cassandraSession.execute(QueryBuilder
      .insertInto(CASSANDRA_TABLE)
      .value(ID_COLUMN_NAME, traceId)
      .value(TIMESTAMP_COLUMN_NAME, new Date())
      .value(SPANS_COLUMN_NAME, ByteBuffer.wrap(spanBuffer.toByteArray)))
  }

  private def createSpanBufferWithSingleSpan(traceId: String,
                                             spanId: String,
                                             serviceName: String,
                                             operationName: String,
                                             tags: Map[String, String]) = {
    val spanTags = tags.map(tag => com.expedia.open.tracing.Tag.newBuilder().setKey(tag._1).setVStr(tag._2).build())

    SpanBuffer
      .newBuilder()
      .setTraceId(traceId)
      .addChildSpans(Span
        .newBuilder()
        .setTraceId(traceId)
        .setSpanId(spanId)
        .setOperationName(operationName)
        .setServiceName(serviceName)
        .addAllTags(spanTags.asJava)
        .build())
      .build()
  }

  protected def putTraceInCassandra(traceId: String,
                                    spanId: String = UUID.randomUUID().toString,
                                    serviceName: String = "",
                                    operationName: String = "",
                                    tags: Map[String, String] = Map.empty): ResultSet = {
    insertTraceInCassandra(traceId, spanId, serviceName, operationName, tags)
  }

  protected def putTraceInCassandraWithPartialSpans(traceId: String): ResultSet = {
    val trace = buildMultiServiceTrace()
    val spanBuffer = SpanBuffer
      .newBuilder()
      .setTraceId(traceId)
      .addAllChildSpans(trace.getChildSpansList)
      .build()

    writeToCassandra(spanBuffer, traceId)
  }
}
