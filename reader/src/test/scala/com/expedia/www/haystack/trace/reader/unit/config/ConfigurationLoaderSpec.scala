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
package com.expedia.www.haystack.trace.reader.unit.config

import com.expedia.www.haystack.trace.commons.config.entities.KeyspaceConfiguration
import com.expedia.www.haystack.trace.reader.config.ProviderConfiguration
import com.expedia.www.haystack.trace.reader.config.entities.{ServiceConfiguration, TraceTransformersConfiguration}
import com.expedia.www.haystack.trace.reader.readers.transformers.{ClientServerEventLogTransformer, DeDuplicateSpanTransformer, PartialSpanTransformer}
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec

class ConfigurationLoaderSpec extends BaseUnitTestSpec {
  describe("ConfigurationLoader") {
    it("should load the service config from base.conf") {
      val serviceConfig: ServiceConfiguration = new ProviderConfiguration().serviceConfig
      serviceConfig.port shouldBe 8088
      serviceConfig.ssl.enabled shouldBe true
      serviceConfig.ssl.certChainFilePath shouldBe "/ssl/cert"
      serviceConfig.ssl.privateKeyPath shouldBe "/ssl/private-key"
      serviceConfig.throttler.size shouldBe 1
      serviceConfig.throttler.get("tracereader/searchtraces") shouldBe Some(10)
    }

    it("should load the trace transformers") {
      val traceConfig: TraceTransformersConfiguration = new ProviderConfiguration().traceTransformerConfig
      traceConfig.postTransformers.length shouldBe 3
      traceConfig.postTransformers.head.isInstanceOf[PartialSpanTransformer] shouldBe true
      traceConfig.preTransformers.length shouldBe 2
      traceConfig.preTransformers.head.isInstanceOf[DeDuplicateSpanTransformer] shouldBe true
      traceConfig.preTransformers(1).isInstanceOf[ClientServerEventLogTransformer] shouldBe true
    }

    it("should load the trace validators") {
      val traceConfig: TraceTransformersConfiguration = new ProviderConfiguration().traceTransformerConfig
      traceConfig.postTransformers.length shouldBe 3
      traceConfig.postTransformers.head.isInstanceOf[PartialSpanTransformer] shouldBe true
      traceConfig.preTransformers.length shouldBe 2
      traceConfig.preTransformers.head.isInstanceOf[DeDuplicateSpanTransformer] shouldBe true
      traceConfig.preTransformers(1).isInstanceOf[ClientServerEventLogTransformer] shouldBe true
    }

    it("should load service metadata configuration ") {
      val metadataConfig = new ProviderConfiguration().serviceMetadataConfig
      metadataConfig.enabled shouldBe false
      metadataConfig.keyspace shouldEqual KeyspaceConfiguration("haystack_metadata", "services")
    }
  }
}
