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

package com.expedia.www.haystack.trace.provider.integration

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Cluster, Session, SimpleStatement}
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.open.tracing.{Process, Span}
import com.expedia.www.haystack.trace.provider.stores.readers.cassandra.Schema._
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.Index
import io.searchbox.indices.CreateIndex
import io.searchbox.params.Parameters
import org.scalatest._

trait BaseIntegrationTestSpec extends FunSpec with GivenWhenThen with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  private val CASSANDRA_ENDPOINT = "cassandra"
  private val CASSANDRA_KEYSPACE = "haystack"
  private val CASSANDRA_TABLE = "traces"

  private val ELASTIC_SEARCH_ENDPOINT = "http://elasticsearch:9200"
  private val SPANS_INDEX_TYPE = "spans"
  private val HAYSTACK_TRACES_INDEX = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    s"haystack-traces-${formatter.format(new Date())}"
  }
  private val INDEX_TEMPLATE =
    s"""
       |{
       |  "template": "haystack-traces*",
       |  "settings": {
       |    "number_of_shards": 1
       |  },
       |  "mappings": {
       |    "spans": {
       |      "_source": {
       |        "enabled": true
       |      },
       |      "properties": {
       |        "spans": {
       |          "type": "nested"
       |        }
       |      }
       |    }
       |  }
       |}
 """.stripMargin

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
  }

  private def deleteCassandraTableRows(): Unit = {
    cassandraSession.execute(new SimpleStatement(s"TRUNCATE ${CASSANDRA_TABLE}"))
  }

  protected def putTraceInCassandraAndEs(traceId: String = UUID.randomUUID().toString,
                                         spanId: String = UUID.randomUUID().toString,
                                         serviceName: String = "",
                                         operationName: String = "") = {
    insertTraceInCassandra(traceId, spanId, serviceName, operationName)
    insertTraceInEs(traceId, spanId, serviceName, operationName)
  }

  private def insertTraceInEs(traceId: String,
                              spanId: String,
                              serviceName: String,
                              operationName: String) = {
    val source =
      s"""
         |{
         |  "duration": 0,
         |  "spans": [{
         |    "service": "$serviceName",
         |    "operation": "$operationName"
         |  }]
         |}
       """.stripMargin

    esClient.execute(new Index.Builder(source)
      .id(s"${traceId}_1234")
      .index(HAYSTACK_TRACES_INDEX)
      .`type`(SPANS_INDEX_TYPE)
      .setParameter(Parameters.OP_TYPE, "create")
      .build)
  }

  private def insertTraceInCassandra(traceId: String,
                                     spanId: String,
                                     serviceName: String,
                                     operationName: String) = {
    val spanBuffer = createSpanBufferWithSingleSpan(traceId, spanId, serviceName, operationName)

    cassandraSession.execute(QueryBuilder
      .insertInto(CASSANDRA_TABLE)
      .value(ID_COLUMN_NAME, traceId)
      .value(TIMESTAMP_COLUMN_NAME, new Date())
      .value(STITCHED_SPANS_COLUMNE_NAME, ByteBuffer.wrap(spanBuffer.toByteArray)))
  }

  private def createSpanBufferWithSingleSpan(traceId: String,
                                             spanId: String,
                                             serviceName: String,
                                             operationName: String) = {
    SpanBuffer
      .newBuilder()
      .setTraceId(traceId)
      .addChildSpans(Span
        .newBuilder()
        .setTraceId(traceId)
        .setSpanId(spanId)
        .setOperationName(operationName)
        .setProcess(Process.newBuilder().setServiceName(serviceName))
        .build())
      .build()
  }

  protected def putTraceInCassandra(traceId: String,
                                    spanId: String = UUID.randomUUID().toString,
                                    serviceName: String = "",
                                    operationName: String = "") = {
    insertTraceInCassandra(traceId, spanId, serviceName, operationName)
  }
}
