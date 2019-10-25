/*
 *  Copyright 2019, Expedia Group.
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

/**
  * defines the configuration parameters for reloading the app configs from external store like ElasticSearch
 *
  * @param enabled: endpoint for external store where app configuration is stored
  * @param region: name of the database
  * @param awsServiceName: app config will be refreshed after this given interval in millis
  * @param accessKey: list of reloadable configuration objects that subscribe to the reloader
  * @param secretKey: loads the app configuration from external store on startup, default is true
  */
case class AWSRequestSigningConfiguration (enabled: Boolean,
                                           region: String,
                                           awsServiceName: String,
                                           accessKey: Option[String],
                                           secretKey: Option[String])
