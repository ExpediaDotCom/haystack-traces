/*
 *  Copyright 2018 Expedia, Inc.
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

package com.expedia.www.haystack.trace.reader.unit.stores.readers

import com.expedia.open.tracing.api.{Field, FieldValuesRequest}
import com.expedia.www.haystack.trace.reader.config.entities.FieldValuesCacheConfiguration
import com.expedia.www.haystack.trace.reader.stores.FieldValuesCache
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class FieldValuesCacheSpec extends BaseUnitTestSpec {
  describe("Field Values Cache") {
    it("should fill the cache when empty and return the response") {
      val field = Field.newBuilder().setName("k1").setValue("v1").build()
      val req = FieldValuesRequest.newBuilder().setFieldName("servicename").addFilters(field).build()
      val expectedOutput = Seq("myservice")

      val config = FieldValuesCacheConfiguration(enabled = true, 100, 2000)
      val cache = new FieldValuesCache(config, (e) => {
        e shouldEqual req
        Future(expectedOutput)
      })

      Thread.sleep(2000)
      validate(cache.get(req), expectedOutput)
      Thread.sleep(2000)
      validate(cache.get(req), expectedOutput)
    }

    it("should return the result even if cache is disabled") {
      val field = Field.newBuilder().setName("k1").setValue("v1").build()
      val req = FieldValuesRequest.newBuilder().setFieldName("servicename").addFilters(field).build()
      val expectedOutput = Seq("myservice")

      val config = FieldValuesCacheConfiguration(enabled = false, 100, 2000)
      val cache = new FieldValuesCache(config, (e) => {
        e shouldEqual req
        Future(expectedOutput)
      })

      var received = cache.get(req)
      Thread.sleep(2000)
      validate(received, expectedOutput)

      received = cache.get(req)
      Thread.sleep(2000)
      validate(cache.get(req), expectedOutput)
    }
  }

  private def validate(resp: Future[Seq[String]], expected: Seq[String]): Unit = {
    resp.onComplete {
      case Success(r) =>  r shouldEqual expected
      case Failure(ex) => fail(ex)
    }
  }
}
