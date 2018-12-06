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

package com.expedia.www.haystack.trace.indexer.integration.clients

import java.util.concurrent.Executors

import com.expedia.open.tracing.backend.{ReadSpansRequest, StorageBackendGrpc, TraceRecord}
import com.expedia.www.haystack.commons.retries.RetryOperation
import com.expedia.www.haystack.trace.commons.config.entities.TraceBackendClientConfiguration
import com.expedia.www.haystack.trace.indexer.config.entities.TraceBackendConfiguration
import com.expedia.www.haystack.trace.indexer.integration.TraceDescription
import com.expedia.www.haystack.trace.storage.backends.memory.Service
import io.grpc.ManagedChannelBuilder

import scala.collection.JavaConverters._

class GrpcTestClient {

  private val executors = Executors.newSingleThreadExecutor()

  val port = 8088

  val storageBackendClient = StorageBackendGrpc.newBlockingStub(ManagedChannelBuilder.forAddress("localhost", port)
    .usePlaintext(true)
    .build())

  def prepare(): Unit = {
    executors.submit(new Runnable {
      override def run(): Unit = Service.main(null)
    })
  }

  def buildConfig = TraceBackendConfiguration(
    TraceBackendClientConfiguration("localhost", port),
    10, RetryOperation.Config(10, 250, 2))

  def queryTraces(traceDescriptions: Seq[TraceDescription]): Seq[TraceRecord] = {
    val traceIds = traceDescriptions.map(traceDescription => traceDescription.traceId).toList
    storageBackendClient.readSpans(ReadSpansRequest.newBuilder().addAllTraceIds(traceIds.asJava).build()).getRecordsList.asScala
  }

}
