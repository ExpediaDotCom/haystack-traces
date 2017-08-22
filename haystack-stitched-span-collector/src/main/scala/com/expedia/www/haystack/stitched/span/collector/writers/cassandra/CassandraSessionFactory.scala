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

import com.datastax.driver.core._
import com.datastax.driver.core.policies._
import com.expedia.www.haystack.stitched.span.collector.config.entities.CassandraConfiguration
import com.expedia.www.haystack.stitched.span.collector.writers.AwsNodeDiscoverer

import scala.util.Try

class CassandraSessionFactory(config: CassandraConfiguration) {

  var cluster: Cluster = _

  /**
    * builds a session object to interact with cassandra cluster
    * Also ensure that keyspace and table names exists in cassandra.
    */
  val session: Session = {
    cluster = buildCluster()
    val newSession = cluster.connect()
    Schema.ensureExists(config.keyspace, config.tableName, config.autoCreateSchema, newSession)
    newSession.execute("USE " + config.keyspace)
    newSession
  }

  /**
    * close the session and client
    */
  def close(): Unit = {
    Try(session.close())
    Try(cluster.close())
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
