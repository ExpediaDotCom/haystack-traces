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
import org.slf4j.LoggerFactory

object Schema {
  private val LOGGER = LoggerFactory.getLogger(Schema.getClass)

  val ID_COLUMN_NAME = "id"
  val TIMESTAMP_COLUMN_NAME = "ts"
  val STITCHED_SPANS_COLUMNE_NAME = "stitchedspans"

  /**
    * ensures the keyspace and table name exists in cassandra
    * @param keyspace cassandra keyspace
    * @param tableName table name in cassandra
    * @param session cassandra client session
    * @param autoCreateSchema if present, then apply the cql schema that should create the keyspace and cassandra table,
    *                         else throw an exception if fail to find the keyspace and table
    */
  def ensureExists(keyspace: String, tableName: String, autoCreateSchema: Option[String], session: Session): Unit = {
    val keyspaceMetadata = session.getCluster.getMetadata.getKeyspace(keyspace)
    if (keyspaceMetadata == null || keyspaceMetadata.getTable(tableName) == null) {
      autoCreateSchema match {
        case Some(schema) => applyCqlSchema(session, schema)
        case _ => throw new RuntimeException(s"Fail to find the keyspace=$keyspace and/or table=$tableName !!!!")
      }
    }
  }

  /**
    * apply the cql schema
    * @param session session object to interact with cassandra
    * @param schema schema data
    */
  private def applyCqlSchema(session: Session, schema: String): Unit = {
    try {
      for (cmd <- schema.split(";")) {
        if (cmd.nonEmpty) session.execute(cmd)
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to apply cql $schema with following reason:", ex)
        throw new RuntimeException(ex)
    }
  }
}
