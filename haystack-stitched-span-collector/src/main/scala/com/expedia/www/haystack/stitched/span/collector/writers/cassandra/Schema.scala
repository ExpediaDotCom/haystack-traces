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

package com.expedia.www.haystack.stitched.span.collector.writers.cassandra

import com.datastax.driver.core.Session
import com.google.common.io.CharStreams
import org.slf4j.LoggerFactory

import scala.io.Source

object Schema {
  val ID_COLUMN_NAME = "id"
  val TIMESTAMP_COLUMN_NAME = "ts"
  val STITCHED_SPANS_COLUMNE_NAME = "stitchedspans"

  private val LOGGER = LoggerFactory.getLogger(Schema.getClass)

  def ensureExists(keyspace: String, tableName: String, session: Session, autoCreateKeySpace: Boolean): Unit = {
    val keyspaceMetadata = session.getCluster.getMetadata.getKeyspace(keyspace)
    if (keyspaceMetadata == null || keyspaceMetadata.getTable(tableName) == null) {
      if (autoCreateKeySpace) {
        applyCqlFile(keyspace, tableName, session, schemaResourcePath)
      }
      else {
        throw new RuntimeException(s"Fail to find the keyspace=$keyspace and/or table=$tableName !!!!")
      }
    }
  }

  private def applyCqlFile(keyspace: String, tableName: String, session: Session, cqlResourcePath: String): Unit = {
    val reader = Source.fromInputStream(getClass.getResourceAsStream(cqlResourcePath)).bufferedReader
    try {
      for (cmd <- CharStreams.toString(reader).split(";")) {
        val execCommand = cmd.trim().replace("{{keyspace}}", keyspace).replace("{{table}}", tableName)
        if (execCommand.nonEmpty) {
          session.execute(execCommand)
        }
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to apply cql file $cqlResourcePath for keyspace=$keyspace and tableName=$tableName. Reason:", ex)
        throw new RuntimeException(ex)
    } finally {
      if (reader != null) {
        reader.close()
      }
    }
  }

  private def schemaResourcePath: String = s"/cassandra-schema/dev.cql"
}
