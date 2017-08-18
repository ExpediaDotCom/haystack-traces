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

package com.expedia.www.haystack.stitch.span.collector.writers.cassandra

import com.datastax.driver.core.policies._
import com.datastax.driver.core._
import com.expedia.www.haystack.stitch.span.collector.config.entities.CassandraConfiguration
import com.expedia.www.haystack.stitch.span.collector.writers.AwsNodeDiscoverer

class CassandraSessionFactory(config: CassandraConfiguration) {

  val session: Session = {
    val  cluster = buildCluster()
    val newSession = cluster.connect()
    Schema.ensureExists(config.keyspace, config.tableName, newSession, config.autoCreateKeyspace)
    newSession.execute("USE " + config.keyspace)
    newSession
  }

  def close(): Unit = {
    if(session != null) {
      session.close()
    }
  }

  private def discoverNodes(): Seq[String] = {
    config.awsNodeDiscovery match {
      case Some(awsDiscovery) => AwsNodeDiscoverer.discover(awsDiscovery.region, awsDiscovery.tags)
      case _ => Nil
    }
  }

  private def buildCluster(): Cluster = {
    val contactPoints = if(config.autoDiscoverEnabled) discoverNodes() else config.endpoints
    require(contactPoints.nonEmpty, "cassandra contact points can't be empty!!!")

    val tokenAwarePolicy = new TokenAwarePolicy(new LatencyAwarePolicy.Builder(new RoundRobinPolicy()).build())

    Cluster.builder()
      .addContactPoints(contactPoints:_*)
      .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
      .withSocketOptions(new SocketOptions()
        .setKeepAlive(config.socket.keepAlive)
        .setConnectTimeoutMillis(config.socket.connectionTimeoutMillis)
        .setReadTimeoutMillis(config.socket.readTimeoutMills))
      .withLoadBalancingPolicy(tokenAwarePolicy)
      .withPoolingOptions(new PoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, config.socket.maxConnectionPerHost))
      .build()
  }
}
