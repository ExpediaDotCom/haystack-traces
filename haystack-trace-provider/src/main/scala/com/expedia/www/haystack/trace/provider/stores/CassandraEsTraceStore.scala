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

package com.expedia.www.haystack.trace.provider.stores

import com.expedia.open.tracing.internal._
import com.expedia.www.haystack.trace.provider.config.entities.{CassandraConfiguration, ElasticSearchConfiguration}
import com.expedia.www.haystack.trace.provider.stores.readers.cassandra.CassandraReader
import com.expedia.www.haystack.trace.provider.stores.readers.es.ElasticSearchReader

import scala.concurrent.{ExecutionContextExecutor, Future}

class CassandraEsTraceStore(cassandraConfiguration: CassandraConfiguration, esConfiguration: ElasticSearchConfiguration)(implicit val executor: ExecutionContextExecutor) extends TraceStore {
  val cassandraReader: CassandraReader = new CassandraReader(cassandraConfiguration)
  val reader: ElasticSearchReader = new ElasticSearchReader(esConfiguration)

  override def getTrace(traceId: String): Future[Trace] = {
    cassandraReader.readTrace(traceId)
  }

  override def searchTraces(request: TracesSearchRequest): Future[TracesSearchResult] = ???
}
