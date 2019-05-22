/*
 *  Copyright 2019 Expedia, Inc.
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

package com.expedia.www.haystack.trace.storage.backends.mysql.client

import java.sql.Connection

import org.slf4j.LoggerFactory

object MysqlTableSchema {
  private val LOGGER = LoggerFactory.getLogger(MysqlTableSchema.getClass)

  val ID_COLUMN_NAME = "id"
  val TIMESTAMP_COLUMN_NAME = "ts"
  val SPANS_COLUMN_NAME = "spans"
  val SERVICE_COLUMN_NAME = "service_name"
  val OPERATION_COLUMN_NAME = "operation_name"

  /**
    * ensures the keyspace and table name exists in com.expedia.www.haystack.trace.storage.backends.mysql
    *
    * @param connection       com.expedia.www.haystack.trace.storage.backends.mysql client connection
    * @param autoCreateSchema if present, then apply the sql schema that should create the keyspace and com.expedia.www.haystack.trace.storage.backends.mysql table
    *
    */
  def ensureExists(autoCreateSchema: Option[String], connection: Connection): Unit = {
    autoCreateSchema match {
      case Some(schema) => applySqlSchema(connection, schema)
      case _ =>
    }
  }

  /**
    * apply the sql schema
    *
    * @param connection session object to interact with com.expedia.www.haystack.trace.storage.backends.mysql
    * @param schema     schema data
    */
  private def applySqlSchema(connection: Connection, schema: String): Unit = {
    val statement = connection.createStatement()
    try {
      for (cmd <- schema.split(";")) {
        if (cmd.nonEmpty) statement.execute(cmd)
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to apply sql $schema with following reason:", ex)
        throw new RuntimeException(ex)
    }
  }
}
