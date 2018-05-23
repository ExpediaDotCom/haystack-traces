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

package com.expedia.www.haystack.trace.indexer.unit

import com.datastax.driver.core.{ConsistencyLevel, PreparedStatement, Statement}
import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraSession
import com.expedia.www.haystack.trace.commons.config.entities.KeyspaceConfiguration
import com.expedia.www.haystack.trace.indexer.writers.cassandra.ServiceMetadataStatementBuilder
import org.easymock.EasyMock
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, Matchers}

class ServiceMetadataStatementBuilderSpec extends FunSpec with Matchers with EasyMockSugar {
  private val TRACE_ID = "trace-1"
  private val SERVICE_NAME = "service_1"
  private val OPERATION_1 = "operation_1"
  private val OPERATION_2 = "operation_2"

  describe("service metadata statement builder ") {
    it ("should not return any statements to execute") {
      val session = mock[CassandraSession]
      val statement = mock[Statement]
      val prepStatement = mock[PreparedStatement]

      val keyspace = KeyspaceConfiguration("haystack_metadata", "services", 100)
      val serviceNameCapture = EasyMock.newCapture[String]()
      val operationsCapture = EasyMock.newCapture[Iterable[String]]()
      val consistencyLevelCapture = EasyMock.newCapture[ConsistencyLevel]()
      val preparedStmtCapture = EasyMock.newCapture[PreparedStatement]()

      expecting {
        session.createServiceMetadataInsertPreparedStatement(keyspace).andReturn(prepStatement)

        session.newServiceMetadataInsertStatement(
          EasyMock.capture(serviceNameCapture),
          EasyMock.capture(operationsCapture),
          EasyMock.capture(consistencyLevelCapture),
          EasyMock.capture(preparedStmtCapture)).andReturn(statement)
      }

      whenExecuting(session, statement) {
        val builder = new ServiceMetadataStatementBuilder(session, keyspace, ConsistencyLevel.ONE)
        val span_1 = Span.newBuilder().setTraceId(TRACE_ID).setServiceName(SERVICE_NAME).setOperationName(OPERATION_1).build()
        val span_2 = Span.newBuilder().setTraceId(TRACE_ID).setServiceName(SERVICE_NAME).setOperationName(OPERATION_2).build()
        val statements = builder.getAndUpdateServiceMetadata(Seq(span_1, span_2), forceWrite = true)
        statements.head shouldBe statement
        serviceNameCapture.getValue shouldBe SERVICE_NAME
        operationsCapture.getValue.toSeq should contain allOf (OPERATION_1, OPERATION_2)
        consistencyLevelCapture.getValue shouldBe ConsistencyLevel.ONE
        preparedStmtCapture.getValue shouldBe prepStatement
      }
    }
  }
}
