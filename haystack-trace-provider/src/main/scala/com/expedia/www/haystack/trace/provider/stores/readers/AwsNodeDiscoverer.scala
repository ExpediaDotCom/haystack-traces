/*
 *  Copyright 2017 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.expedia.www.haystack.trace.provider.stores.readers

import java.util.Collections

import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.{DescribeInstancesRequest, Filter, Instance, InstanceStateName}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object AwsNodeDiscoverer {
  private val LOGGER = LoggerFactory.getLogger(AwsNodeDiscoverer.getClass)

  def discover(client: AmazonEC2Client, tags: Map[String, String]): Seq[String] = {
    val filters = tags.map { case (key, value) => new Filter("tag:" + key, Collections.singletonList(value)) }
    val request = new DescribeInstancesRequest().withFilters(filters)

    val result = client.describeInstances(request)

    val nodes = result.getReservations
      .flatMap(_.getInstances)
      .filter(isValidInstance)
      .map(_.getPrivateIpAddress)

    LOGGER.info("ec2 nodes discovered [{}]", nodes.mkString(","))
    nodes
  }

  def discover(region: String,
               tags: Map[String, String]): Seq[String] = {
    LOGGER.info(s"discovering ec2 nodes for region=$region, and tags=${tags.mkString(",")}")

    val awsRegion = Region.getRegion(Regions.fromName(region))
    val client:AmazonEC2Client = new AmazonEC2Client().withRegion(awsRegion)
    try {
      discover(client, tags)
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Fail to discover ec2 nodes for region=$region and tags=$tags with reason", ex)
        throw new RuntimeException(ex)
    } finally {
      client.shutdown()
    }
  }

  private def isValidInstance(instance: Instance): Boolean = {
    // instance should be in running state
    InstanceStateName.Running.toString.equals(instance.getState.getName)
  }
}
