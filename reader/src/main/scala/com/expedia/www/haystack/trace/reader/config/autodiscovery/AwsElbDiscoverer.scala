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

package com.expedia.www.haystack.trace.reader.config.autodiscovery

import java.util.Collections

import com.amazonaws.regions.Regions
import com.amazonaws.services.elasticloadbalancing.{AmazonElasticLoadBalancingClientBuilder, AmazonElasticLoadBalancing}
import com.amazonaws.services.resourcegroupstaggingapi.{AWSResourceGroupsTaggingAPIClientBuilder, AWSResourceGroupsTaggingAPI}
import com.amazonaws.services.resourcegroupstaggingapi.model.{GetResourcesRequest, TagFilter}
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object AwsElbDiscoverer {
  private val LOGGER = LoggerFactory.getLogger(AwsElbDiscoverer.getClass)

  def getName(arn:String) : String = {
    // for fetching name from arn similar to arn:partition:service:region:account-id:resource-type/resource-id
    // Sample for our case resource-type/resource-id : elasticloadbalancing/<load balancer name>
    val resourceIdentifier = arn.split(":")(5)
    resourceIdentifier.split("/")(1)
  }

  /**
   * discovers the ELB DNS Names on AWS for a given region and set of tags
   * @param region aws region
   * @param tags a set of elb tags
   * @return
   */

  def discoverElbs(region: String,
                   tags: Map[String, String]): Seq[String] = {
    LOGGER.info(s"discovering ELB arn for region=$region, and tags=${tags.mkString(",")}")

    val awsRegion = Regions.fromName(region)
    val resourceGroupTaggingClient : AWSResourceGroupsTaggingAPI = AWSResourceGroupsTaggingAPIClientBuilder.standard().withRegion(awsRegion).build();

    try {
      val filters = tags.map { case (key, value) => new TagFilter().withKey(key).withValues(Collections.singletonList(value)) }

      val filterElbRequest = new GetResourcesRequest().withResourceTypeFilters(AmazonElasticLoadBalancing.ENDPOINT_PREFIX).withTagFilters(filters.asJavaCollection)

      val filterElbResult = resourceGroupTaggingClient.getResources(filterElbRequest)

      val elbArns = filterElbResult.getResourceTagMappingList
        .asScala
        .map(_.getResourceARN)

      val elbNames = elbArns.map(arn => getName(arn))

      getElbDnsNames(elbNames, awsRegion)

    } catch {
      case ex: Exception =>
        LOGGER.error(s"Fail to discover ELB arns for region=$region and tags=$tags with reason", ex)
        throw new RuntimeException(ex)
    } finally {
      resourceGroupTaggingClient.shutdown()
    }
  }

  /**
   * get ELB DNS Names on AWS for a given region and list of ELB Names
   * @param awsRegion aws region
   * @param elbNames a list of elb names
   * @return
   */

  def getElbDnsNames(elbNames: Seq[String], awsRegion: Regions): Seq[String] = {
    LOGGER.info(s"fetching ELB DNS Names for elbs=${elbNames.mkString(",")}")

    val elbClient : AmazonElasticLoadBalancing = AmazonElasticLoadBalancingClientBuilder.standard().withRegion(awsRegion).build()

    try {
      val describeElbRequest = new DescribeLoadBalancersRequest().withLoadBalancerNames(elbNames.asJava)

      val describeElbresult = elbClient.describeLoadBalancers(describeElbRequest)

      describeElbresult.getLoadBalancerDescriptions
        .asScala
        .map(_.getDNSName)

    } catch {
      case ex: Exception =>
        LOGGER.error(s"Fail to get ELB DNS Names for region=${awsRegion.toString} and tags=$elbNames with reason", ex)
        throw new RuntimeException(ex)
    } finally {
      elbClient.shutdown()
    }
  }
}
