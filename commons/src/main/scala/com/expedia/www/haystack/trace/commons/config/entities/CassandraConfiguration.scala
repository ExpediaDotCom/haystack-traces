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

package com.expedia.www.haystack.trace.commons.config.entities

import org.apache.commons.lang3.StringUtils


/** define the keyspace and table information in cassandra
  *
  * @param name              : name of cassandra keyspace
  * @param table             : name of cassandra table
  * @param autoCreateSchema  : apply cql and create keyspace and tables if not exist, optional
  * @param recordTTLInSec    : ttl of record in sec
  */
case class KeyspaceConfiguration(name: String, table: String, autoCreateSchema: Option[String] = None, recordTTLInSec: Long = -1) {
  require(StringUtils.isNotEmpty(name))
  require(StringUtils.isNotEmpty(table))
}

/**
  * defines the configuration parameters for cassandra
  *
  * @param endpoints                : list of cassandra endpoints
  * @param autoDiscoverEnabled      : if autodiscovery is enabled, then 'endpoints' config parameter will be ignored
  * @param awsNodeDiscovery         : discovery configuration for aws, optional. This is applied only if autoDiscoverEnabled is true
  * @param spanKeyspace             : cassandra keyspace for spans
  * @param serviceMetadataKeyspace  : cassandra keyspace for service metadata
  * @param socket                   : socket configuration like maxConnections, timeouts and keepAlive
  */
case class CassandraConfiguration(endpoints: List[String],
                                  autoDiscoverEnabled: Boolean,
                                  awsNodeDiscovery: Option[AwsNodeDiscoveryConfiguration],
                                  plaintextCredentials: Option[CredentialsConfiguration],
                                  spanKeyspace: KeyspaceConfiguration,
                                  serviceMetadataKeyspace: KeyspaceConfiguration,
                                  socket: SocketConfiguration)
