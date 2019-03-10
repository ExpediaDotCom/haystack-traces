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

package com.expedia.www.haystack.trace.storage.backends.mysql.config.entities

import com.expedia.www.haystack.commons.retries.RetryOperation
import org.apache.commons.lang3.StringUtils


/** define the table information in mysql
  *
  * @param name             : name of mysql table
  * @param recordTTLInSec   : ttl of record in sec
  * @param autoCreateSchema : apply sql and create table if not exist, optional
  */
case class DatabaseConfiguration(name: String,
                                 recordTTLInSec: Int = -1,
                                 autoCreateSchema: Option[String] = None) {
  require(StringUtils.isNotEmpty(name))
}

/**
  * defines the configuration parameters for mysql client
  *
  * @param endpoints    : list of mysql endpoints
  * @param driver : Sql Driver Implementation Class
  * @param socket : socket configuration like maxConnections, timeouts and keepAlive
  */
case class ClientConfiguration(endpoints: String,
                               driver: String,
                               plaintextCredentials: Option[CredentialsConfiguration],
                               socket: SocketConfiguration)

/**
  * @param retryConfig retry configuration if writes fail
  */
case class MysqlConfiguration(clientConfig: ClientConfiguration,
                              retryConfig: RetryOperation.Config,
                              databaseConfig: DatabaseConfiguration
                             )

