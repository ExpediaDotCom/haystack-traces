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
import com.amazonaws.services.elasticsearch.{AWSElasticsearchClientBuilder, AWSElasticsearch}
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainsRequest
import com.amazonaws.services.resourcegroupstaggingapi.{AWSResourceGroupsTaggingAPI, AWSResourceGroupsTaggingAPIClientBuilder}
import com.amazonaws.services.resourcegroupstaggingapi.model.{GetResourcesRequest, TagFilter}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object AwsElasticSearchDiscoverer {
  private val LOGGER = LoggerFactory.getLogger(AwsElasticSearchDiscoverer.getClass)

  def getName(arn:String) : String = {
    // for fetching name from arn similar to arn:partition:service:region:account-id:resource-type/resource-id
    // Sample for our case resource-type/resource-id : domain/<elastic search name>
    val resourceIdentifier = arn.split(":")(5)
    resourceIdentifier.split("/")(1)
  }

  /**
   * discovers the Elasticsearch VPC Endpoints on AWS for a given region and set of tags
   * @param region aws region
   * @param tags a set of elb tags
   * @return
   */

  def discoverElasticSearchDomains(region: String,
                                   tags: Map[String, String]): Seq[String] = {
    LOGGER.info(s"discovering ES arn for region=$region, and tags=${tags.mkString(",")}")

    val awsRegion = Regions.fromName(region)
    val resourceGroupTaggingClient : AWSResourceGroupsTaggingAPI = AWSResourceGroupsTaggingAPIClientBuilder.standard().withRegion(awsRegion).build();

    try {
      val filters = tags.map { case (key, value) => new TagFilter().withKey(key).withValues(Collections.singletonList(value)) }

      val filterEsRequest = new GetResourcesRequest().withResourceTypeFilters(AWSElasticsearch.ENDPOINT_PREFIX).withTagFilters(filters.asJavaCollection)

      val filterEsresult = resourceGroupTaggingClient.getResources(filterEsRequest)

      val esArns = filterEsresult.getResourceTagMappingList
        .asScala
        .map(_.getResourceARN)

      val esNames = esArns.map(arn => getName(arn))

      getElasticSearchEndpoints(esNames, awsRegion)

    } catch {
      case ex: Exception =>
        LOGGER.error(s"Fail to discover ElasticSearch arns for region=$region and tags=$tags with reason", ex)
        throw new RuntimeException(ex)
    } finally {
      resourceGroupTaggingClient.shutdown()
    }
  }

  /**
   * get Elasticsearch VPC Endpoints on AWS for a given region and list of ELB Names
   * @param awsRegion aws region
   * @param esNames a list of elb names
   * @return
   */

  def getElasticSearchEndpoints(esNames: Seq[String], awsRegion: Regions): Seq[String] = {
    LOGGER.info(s"fetching Elasticsearch Endpoints for es=${esNames.mkString(",")}")

    val esClient : AWSElasticsearch = AWSElasticsearchClientBuilder.standard().withRegion(awsRegion).build()

    try {
      val describeEsRequest = new DescribeElasticsearchDomainsRequest().withDomainNames(esNames.asJava)

      val describeEsResult = esClient.describeElasticsearchDomains(describeEsRequest)

      describeEsResult.getDomainStatusList
        .asScala
        .map(_.getEndpoints.get("vpc"))

    } catch {
      case ex: Exception =>
        LOGGER.error(s"Fail to get Elasticsearch Endpoints for region=${awsRegion.toString} and tags=$esNames with reason", ex)
        throw new RuntimeException(ex)
    } finally {
      esClient.shutdown()
    }
  }
}
