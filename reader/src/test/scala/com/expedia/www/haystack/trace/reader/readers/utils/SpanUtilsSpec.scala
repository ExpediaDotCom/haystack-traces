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
package com.expedia.www.haystack.trace.reader.readers.utils

import java.lang.System.currentTimeMillis

import com.expedia.open.tracing.Span
import com.expedia.open.tracing.Tag
import com.expedia.www.haystack.trace.reader.readers.utils.SpanMarkers.SERVICE_TAG_KEY
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec

class SpanUtilsSpec extends BaseUnitTestSpec {
  val ServiceNameInAttribute: String = currentTimeMillis + "ServiceNameInAttribute"
  val ServiceNameInTag: String = currentTimeMillis + "ServiceNameInTag"

  describe("SpanUtils object, getEffectiveServiceName() method") {
    it("should return the span's service name if the 'service' tag is not found") {
      val span = Span.newBuilder().setServiceName(ServiceNameInAttribute).build
      val serviceName = SpanUtils.getEffectiveServiceName(span)
      assert(serviceName == ServiceNameInAttribute)
    }

    it("should return the tag's service name if the 'service' tag has a non-null value") {
      val tag = Tag.newBuilder().setKey(SERVICE_TAG_KEY).setVStr(ServiceNameInTag).build
      val span = Span.newBuilder().setServiceName(ServiceNameInAttribute).addTags(tag).build
      val serviceName = SpanUtils.getEffectiveServiceName(span)
      assert(serviceName == ServiceNameInTag)
    }

    it("should return the span's service name if the 'service' tag has no value") {
      val tag = Tag.newBuilder().setKey(SERVICE_TAG_KEY).build
      val span = Span.newBuilder().setServiceName(ServiceNameInAttribute).addTags(tag).build
      val serviceName = SpanUtils.getEffectiveServiceName(span)
      assert(serviceName == ServiceNameInAttribute)
    }
  }
}
