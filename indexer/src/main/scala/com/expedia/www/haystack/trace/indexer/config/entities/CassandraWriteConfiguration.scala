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

package com.expedia.www.haystack.trace.indexer.config.entities

import com.datastax.driver.core.ConsistencyLevel
import com.expedia.www.haystack.commons.retries.RetryOperation
import com.expedia.www.haystack.trace.commons.config.entities.CassandraConfiguration

/**
  * @param consistencyLevel: consistency level of writes
  * @param recordTTLInSec: record ttl in seconds
  * @param maxInFlightRequests defines the max parallel writes to cassandra
  * @param retryConfig retry configuration if writes fail
  * @param consistencyLevelOnError: downgraded consistency level on write error
  */
case class CassandraWriteConfiguration(clientConfig: CassandraConfiguration,
                                       consistencyLevel: ConsistencyLevel,
                                       recordTTLInSec: Int,
                                       maxInFlightRequests: Int,
                                       retryConfig: RetryOperation.Config,
                                       consistencyLevelOnError: List[(Class[_], ConsistencyLevel)]) {
  def writeConsistencyLevel(error: Throwable): ConsistencyLevel = {
    if (error == null) {
      consistencyLevel
    } else {
      consistencyLevelOnError
        .find(errorClass => errorClass._1.isAssignableFrom(error.getClass))
        .map(_._2)
        .getOrElse(consistencyLevel)
    }
  }
}
