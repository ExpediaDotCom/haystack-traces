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
import java.util.Date

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Cluster, Session, SimpleStatement}
import com.expedia.open.tracing.Span
import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.trace.provider.stores.readers.cassandra.Schema._
import org.scalatest._

trait BaseIntegrationTestSpec extends FunSpec with GivenWhenThen with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  private val CASSANDRA_ENDPOINT = "cassandra"
  private val CASSANDRA_KEYSPACE = "haystack"
  private val CASSANDRA_TABLE = "traces"

  private var cassandraSession: Session = _

  override def beforeAll() {
    cassandraSession = Cluster.builder().addContactPoints(CASSANDRA_ENDPOINT).build().connect(CASSANDRA_KEYSPACE)
    deleteCassandraTableRows()
  }

  private def deleteCassandraTableRows(): Unit = {
    cassandraSession.execute(new SimpleStatement(s"TRUNCATE ${CASSANDRA_TABLE}"))
  }

  protected def putTraceInCassandra(traceId: String, spanId: String = "") = {
    val stitchedSpan = StitchedSpan
      .newBuilder()
      .addChildSpans(Span
        .newBuilder()
        .setTraceId(traceId)
        .setSpanId(spanId)
        .build())
      .setTraceId(traceId)
      .build()

    cassandraSession.execute(QueryBuilder
      .insertInto(CASSANDRA_TABLE)
      .value(ID_COLUMN_NAME, traceId)
      .value(TIMESTAMP_COLUMN_NAME, new Date())
      .value(STITCHED_SPANS_COLUMNE_NAME, ByteBuffer.wrap(stitchedSpan.toByteArray)))
  }
}
