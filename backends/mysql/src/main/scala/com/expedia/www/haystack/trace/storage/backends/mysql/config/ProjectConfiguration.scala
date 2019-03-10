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

package com.expedia.www.haystack.trace.storage.backends.mysql.config

import com.expedia.www.haystack.commons.config.ConfigurationLoader
import com.expedia.www.haystack.commons.retries.RetryOperation
import com.expedia.www.haystack.trace.storage.backends.mysql.config.entities._
import com.typesafe.config.Config
import org.apache.commons.lang3.StringUtils

class ProjectConfiguration {
  private val config = ConfigurationLoader.loadConfigFileWithEnvOverrides()

  val healthStatusFilePath: String = config.getString("health.status.path")

  val serviceConfig: ServiceConfiguration = {
    val serviceConfig = config.getConfig("service")

    val ssl = serviceConfig.getConfig("ssl")
    val sslConfig = SslConfiguration(ssl.getBoolean("enabled"), ssl.getString("cert.path"), ssl.getString("private.key.path"))

    ServiceConfiguration(serviceConfig.getInt("port"), sslConfig)
  }
  /**
    *
    * mysql configuration object
    */
  val mysqlConfig: MysqlConfiguration = {


    def databaseConfig(databaseConfig: Config): DatabaseConfiguration = {
      val autoCreateSchemaField = "auto.create.schema"
      val autoCreateSchema: Option[String] = if (databaseConfig.hasPath(autoCreateSchemaField)
        && StringUtils.isNotEmpty(databaseConfig.getString(autoCreateSchemaField))) {
        Some(databaseConfig.getString(autoCreateSchemaField))
      } else {
        None
      }

      DatabaseConfiguration(databaseConfig.getString("name"), databaseConfig.getInt("ttl.sec"), autoCreateSchema)
    }

    val mysqlConfig = config.getConfig("mysql")


    val credentialsConfig: Option[CredentialsConfiguration] =
      if (mysqlConfig.hasPath("credentials")) {
        Some(CredentialsConfiguration(mysqlConfig.getString("credentials.username"), mysqlConfig.getString("credentials.password")))
      } else {
        None
      }

    val socketConfig = mysqlConfig.getConfig("connections")

    val socket = SocketConfiguration(
      socketConfig.getInt("max.per.host"),
      socketConfig.getBoolean("keep.alive"),
      socketConfig.getInt("conn.timeout.ms"),
      socketConfig.getInt("read.timeout.ms"))

    MysqlConfiguration(
      clientConfig = ClientConfiguration(
        mysqlConfig.getString("endpoints"),
        mysqlConfig.getString("driver"),
        credentialsConfig,
        socket),
      retryConfig = RetryOperation.Config(
        mysqlConfig.getInt("retries.max"),
        mysqlConfig.getLong("retries.backoff.initial.ms"),
        mysqlConfig.getDouble("retries.backoff.factor")),
      databaseConfig =  databaseConfig(mysqlConfig.getConfig("database"))
    )
  }

}
