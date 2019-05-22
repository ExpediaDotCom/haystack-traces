/*
 *  Copyright 2019 Expedia, Inc.
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

package com.expedia.www.haystack.trace.storage.backends.mysql.integration

import java.util.UUID
import java.util.concurrent.Executors

import com.expedia.open.tracing.backend.{StorageBackendGrpc, TraceRecord}
import com.expedia.www.haystack.trace.storage.backends.mysql.Service
import com.google.protobuf.ByteString
import io.grpc.ManagedChannelBuilder
import io.grpc.health.v1.HealthGrpc
import org.scalatest._

trait BaseIntegrationTestSpec extends FunSpec with GivenWhenThen with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  protected var client: StorageBackendGrpc.StorageBackendBlockingStub = _

  protected var healthCheckClient: HealthGrpc.HealthBlockingStub = _

  private val executors = Executors.newSingleThreadExecutor()


  override def beforeAll() {



    executors.submit(new Runnable {
      override def run(): Unit = Service.main(null)
    })

    Thread.sleep(5000)

    client = StorageBackendGrpc.newBlockingStub(ManagedChannelBuilder.forAddress("localhost", 8090)
      .usePlaintext(true)
      .build())

    healthCheckClient = HealthGrpc.newBlockingStub(ManagedChannelBuilder.forAddress("localhost", 8090)
      .usePlaintext(true)
      .build())
  }


  protected def createTraceRecord(traceId: String = UUID.randomUUID().toString,
                                 ): TraceRecord = {
    val spans = "random span".getBytes
    TraceRecord
      .newBuilder()
      .setTraceId(traceId)
      .setTimestamp(System.currentTimeMillis())
      .setSpans(ByteString.copyFrom(spans)).build()
  }
}
