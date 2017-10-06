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

package com.expedia.www.haystack.trace.commons.unit

import java.nio.ByteBuffer

import com.datastax.driver.core._
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraTableSchema
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, Matchers}

class CassandraTableSchemaSpec extends FunSpec with Matchers with EasyMockSugar {

  describe("Cassandra Table Schema") {
    it("should extract valid spanBuffer from row data") {
      val row = mock[Row]
      val spanBufferBytes = SpanBuffer.newBuilder().setTraceId("trace-id").build().toByteArray
      expecting {
        row.getBytes(CassandraTableSchema.SPANS_COLUMN_NAME).andReturn(ByteBuffer.wrap(spanBufferBytes))
      }
      whenExecuting(row) {
        val mayBeSpanBuffer = CassandraTableSchema.extractSpanBufferFromRow(row)
        mayBeSpanBuffer.isSuccess shouldBe true
        mayBeSpanBuffer.get.toByteArray shouldEqual spanBufferBytes
      }
    }

    it("should return error if invalid spanBuffer is present in row data") {
      val row = mock[Row]
      val spanBufferBytes = "abc".getBytes("utf-8")
      expecting {
        row.getBytes(CassandraTableSchema.SPANS_COLUMN_NAME).andReturn(ByteBuffer.wrap(spanBufferBytes))
      }
      whenExecuting(row) {
        val mayBeSpanBuffer = CassandraTableSchema.extractSpanBufferFromRow(row)
        mayBeSpanBuffer.isSuccess shouldBe false
      }
    }

    it("should apply the schema if table does not exist in cassandra") {
      val session = mock[Session]
      val cluster = mock[Cluster]
      val metadata = mock[Metadata]
      val keyspaceMetadata = mock[KeyspaceMetadata]
      val keyspace = "my-keyspace"
      val cassandraTableName = "my-table"

      expecting {
        session.execute("apply schema").andReturn(null).once
        keyspaceMetadata.getTable(cassandraTableName).andReturn(null).once()
        metadata.getKeyspace(keyspace).andReturn(keyspaceMetadata).once()
        cluster.getMetadata.andReturn(metadata).once()
        session.getCluster.andReturn(cluster).once()
      }
      whenExecuting(session, cluster, metadata, keyspaceMetadata) {
        CassandraTableSchema.ensureExists(keyspace, cassandraTableName, Some("apply schema"), session)
      }
    }

    it("should apply the schema if keyspace and table does not exist in cassandra") {
      val session = mock[Session]
      val cluster = mock[Cluster]
      val metadata = mock[Metadata]
      val keyspace = "my-keyspace"
      val cassandraTableName = "my-table"

      expecting {
        session.execute("apply schema").andReturn(null).once
        session.execute("apply schema2").andReturn(null).once
        metadata.getKeyspace(keyspace).andReturn(null).once()
        cluster.getMetadata.andReturn(metadata).once()
        session.getCluster.andReturn(cluster).once()
      }
      whenExecuting(session, cluster, metadata) {
        CassandraTableSchema.ensureExists(keyspace, cassandraTableName, Some("apply schema;apply schema2"), session)
      }
    }

    it("should not apply the schema if keyspace and table both exists in cassandra") {
      val session = mock[Session]
      val cluster = mock[Cluster]
      val metadata = mock[Metadata]
      val keyspaceMetadata = mock[KeyspaceMetadata]
      val tableMetadata = mock[TableMetadata]

      val keyspace = "my-keyspace"
      val cassandraTableName = "my-table"

      expecting {
        keyspaceMetadata.getTable(cassandraTableName).andReturn(tableMetadata).once()
        metadata.getKeyspace(keyspace).andReturn(keyspaceMetadata).once()
        cluster.getMetadata.andReturn(metadata).once()
        session.getCluster.andReturn(cluster).once()
      }
      whenExecuting(session, cluster, metadata, keyspaceMetadata, tableMetadata) {
        CassandraTableSchema.ensureExists(keyspace, cassandraTableName, Some("apply schema"), session)
      }
    }

    it("should throw an exception if keyspace and table does not exists in cassandra and no schema is applied") {
      val session = mock[Session]
      val cluster = mock[Cluster]
      val metadata = mock[Metadata]
      val keyspaceMetadata = mock[KeyspaceMetadata]

      val keyspace = "my-keyspace"
      val cassandraTableName = "my-table"

      expecting {
        keyspaceMetadata.getTable(cassandraTableName).andReturn(null).once()
        metadata.getKeyspace(keyspace).andReturn(keyspaceMetadata).once()
        cluster.getMetadata.andReturn(metadata).once()
        session.getCluster.andReturn(cluster).once()
      }
      whenExecuting(session, cluster, metadata, keyspaceMetadata) {
        val thrown = intercept[Exception] {
          CassandraTableSchema.ensureExists(keyspace, cassandraTableName, None, session)
        }
        thrown.getMessage shouldEqual s"Fail to find the keyspace=$keyspace and/or table=$cassandraTableName !!!!"
      }
    }

    it("should thrown an exception if fail to apply the schema when keyspace/table does not exist in cassandra") {
      val session = mock[Session]
      val cluster = mock[Cluster]
      val metadata = mock[Metadata]
      val applySchemaException = new RuntimeException
      val keyspace = "my-keyspace"
      val cassandraTableName = "my-table"

      expecting {
        session.execute("apply schema").andThrow(applySchemaException)
        metadata.getKeyspace(keyspace).andReturn(null).once()
        cluster.getMetadata.andReturn(metadata).once()
        session.getCluster.andReturn(cluster).once()
      }
      whenExecuting(session, cluster, metadata) {
        val thrown = intercept[Exception] {
          CassandraTableSchema.ensureExists(keyspace, cassandraTableName, Some("apply schema;apply schema2"), session)
        }
        thrown.getCause shouldBe applySchemaException
      }
    }
  }
}
