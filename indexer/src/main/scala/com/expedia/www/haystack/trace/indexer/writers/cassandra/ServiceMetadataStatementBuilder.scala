/*
 *  Copyright 2018 Expedia, Inc.
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

package com.expedia.www.haystack.trace.indexer.writers.cassandra

import java.time.Instant

import com.datastax.driver.core.Statement
import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraSession
import com.expedia.www.haystack.trace.commons.utils.SpanUtils
import com.expedia.www.haystack.trace.indexer.config.entities.ServiceMetadataWriteConfiguration
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable

/**
  * builder that generates cassandra statements for writing serviceName and its operations
  * @param cassandra: cassandra session
  * @param cfg: service metadata configuration
  */
class ServiceMetadataStatementBuilder(cassandra: CassandraSession,
                                      cfg: ServiceMetadataWriteConfiguration) {
  private var serviceMetadataMap = new mutable.HashMap[String, mutable.Set[String]]()
  private val serviceMetadataInsertPreparedStmt = cassandra.createServiceMetadataInsertPreparedStatement(cfg.cassandraKeyspace)
  private var allOperationCount: Int = 0
  private var lastFlushInstant = Instant.MIN

  private def shouldFlush: Boolean = {
    cfg.flushIntervalInSec == 0 || Instant.now().minusSeconds(cfg.flushIntervalInSec).isAfter(lastFlushInstant)
  }

  private def areStatementsReadyToBeExecuted(): Seq[Statement] = {
    if (serviceMetadataMap.nonEmpty && (shouldFlush || allOperationCount > cfg.flushOnMaxOperationCount)) {
      val statements = serviceMetadataMap.map {
        case (serviceName, operationList) =>
          cassandra.newServiceMetadataInsertStatement(
            serviceName,
            operationList,
            cfg.consistencyLevel,
            serviceMetadataInsertPreparedStmt)
      }

      lastFlushInstant = Instant.now()
      serviceMetadataMap = new mutable.HashMap[String, mutable.Set[String]]()
      allOperationCount = 0
      statements.toSeq
    } else {
      Nil
    }
  }

  /**
    * get or update the cassandra statements that need to be executed
    *
    * @param spans: list of spans
    * @return
    */
  def getAndUpdateServiceMetadata(spans: Iterable[Span]): Seq[Statement] = {
    this.synchronized {
      spans.foreach(span => {
        val serviceName = SpanUtils.getEffectiveServiceName(span)
        if (StringUtils.isNotEmpty(serviceName) && StringUtils.isNotEmpty(span.getOperationName)) {
          val operationsList = serviceMetadataMap.getOrElseUpdate(serviceName, mutable.Set[String]())
          if (operationsList.add(span.getOperationName)) {
            allOperationCount += 1
          }
        }
      })
      areStatementsReadyToBeExecuted()
    }
  }
}
