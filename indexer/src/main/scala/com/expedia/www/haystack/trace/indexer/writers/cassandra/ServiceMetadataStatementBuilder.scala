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

import com.datastax.driver.core.{ConsistencyLevel, Statement}
import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trace.commons.clients.cassandra.CassandraSession
import com.expedia.www.haystack.trace.commons.config.entities.KeyspaceConfiguration
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable

class ServiceMetadataStatementBuilder(cassandra: CassandraSession,
                                      keyspace: KeyspaceConfiguration,
                                      consistencyLevel: ConsistencyLevel) {
  private var serviceMetadataMap = new mutable.HashMap[String, mutable.Set[String]]()
  private val serviceMetadataInsertPreparedStmt = cassandra.createServiceMetadataInsertPreparedStatement(keyspace)

  def getAndUpdateServiceMetadata(spans: Iterable[Span], forceWrite: Boolean): Seq[Statement] = {
    this.synchronized {
      spans.foreach(span => {
        if (StringUtils.isNotEmpty(span.getServiceName) && StringUtils.isNotEmpty(span.getOperationName)) {
          val operationsList = serviceMetadataMap.getOrElseUpdate(span.getServiceName, mutable.Set[String]())
          operationsList.add(span.getOperationName)
        }
      })
      if (forceWrite) {
        val statements = serviceMetadataMap.map {
          case (serviceName, operationList) =>
            cassandra.newServiceMetadataInsertStatement(
              serviceName,
              operationList,
              consistencyLevel,
              serviceMetadataInsertPreparedStmt)
        }
        serviceMetadataMap = new mutable.HashMap[String, mutable.Set[String]]()
        statements.toSeq
      } else {
        Nil
      }
    }
  }
}
