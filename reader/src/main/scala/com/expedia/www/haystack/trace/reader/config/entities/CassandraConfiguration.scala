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
package com.expedia.www.haystack.trace.reader.config.entities

case class AwsNodeDiscoveryConfiguration(region: String, tags: Map[String, String])

case class SocketConfiguration(maxConnectionPerHost: Int,
                               keepAlive: Boolean,
                               connectionTimeoutMillis: Int,
                               readTimeoutMills: Int)

case class CassandraConfiguration(endpoints: List[String],
                                  autoDiscoverEnabled: Boolean,
                                  awsNodeDiscovery: Option[AwsNodeDiscoveryConfiguration],
                                  keyspace: String,
                                  tableName: String,
                                  socket: SocketConfiguration)
