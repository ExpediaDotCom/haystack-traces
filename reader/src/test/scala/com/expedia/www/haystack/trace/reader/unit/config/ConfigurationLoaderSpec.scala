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

import com.expedia.www.haystack.trace.reader.config.ProviderConfiguration
import com.expedia.www.haystack.trace.reader.config.entities.{ServiceConfiguration, TraceTransformersConfiguration}
import com.expedia.www.haystack.trace.reader.readers.transformers.{ClientServerEventLogTransformer, DeDuplicateSpanTransformer, InfrastructureTagTransformer, PartialSpanTransformer}
import com.expedia.www.haystack.trace.reader.unit.BaseUnitTestSpec

class ConfigurationLoaderSpec extends BaseUnitTestSpec {
  describe("ConfigurationLoader") {
    it("should load the service config from base.conf") {
      val serviceConfig: ServiceConfiguration = new ProviderConfiguration().serviceConfig
      serviceConfig.port shouldBe 8088
      serviceConfig.ssl.enabled shouldBe true
      serviceConfig.ssl.certChainFilePath shouldBe "/ssl/cert"
      serviceConfig.ssl.privateKeyPath shouldBe "/ssl/private-key"
    }

    it("should load the trace transformers") {
      val traceConfig: TraceTransformersConfiguration = new ProviderConfiguration().traceTransformerConfig
      traceConfig.postTransformers.length shouldBe 3
      traceConfig.postTransformers.head.isInstanceOf[PartialSpanTransformer] shouldBe true
      traceConfig.preTransformers.length shouldBe 3
      traceConfig.preTransformers.head.isInstanceOf[DeDuplicateSpanTransformer] shouldBe true
      traceConfig.preTransformers(1).isInstanceOf[ClientServerEventLogTransformer] shouldBe true
      traceConfig.preTransformers(2).isInstanceOf[InfrastructureTagTransformer] shouldBe true
    }

    it("should load the trace validators") {
      val traceConfig: TraceTransformersConfiguration = new ProviderConfiguration().traceTransformerConfig
      traceConfig.postTransformers.length shouldBe 3
      traceConfig.postTransformers.head.isInstanceOf[PartialSpanTransformer] shouldBe true
      traceConfig.preTransformers.length shouldBe 3
      traceConfig.preTransformers.head.isInstanceOf[DeDuplicateSpanTransformer] shouldBe true
      traceConfig.preTransformers(1).isInstanceOf[ClientServerEventLogTransformer] shouldBe true
      traceConfig.preTransformers(2).isInstanceOf[InfrastructureTagTransformer] shouldBe true
    }

    it("should load elastic search configuration") {


      val elasticSearchConfig = new ProviderConfiguration().elasticSearchConfiguration

      elasticSearchConfig.clientConfiguration.endpoint shouldEqual "http://elasticsearch:9200"
      elasticSearchConfig.clientConfiguration.connectionTimeoutMillis shouldEqual 10000
      elasticSearchConfig.clientConfiguration.readTimeoutMillis shouldEqual 5000


      elasticSearchConfig.spansIndexConfiguration.indexHourBucket shouldEqual 6
      elasticSearchConfig.spansIndexConfiguration.indexHourTtl shouldEqual 72
      elasticSearchConfig.spansIndexConfiguration.useRootDocumentStartTime shouldEqual false
      elasticSearchConfig.spansIndexConfiguration.indexType shouldEqual "spans"
      elasticSearchConfig.spansIndexConfiguration.indexNamePrefix shouldEqual "haystack-traces"


      elasticSearchConfig.serviceMetadataIndexConfiguration.enabled shouldEqual false
      elasticSearchConfig.serviceMetadataIndexConfiguration.indexName shouldEqual "service_metadata"
      elasticSearchConfig.serviceMetadataIndexConfiguration.indexType shouldEqual "metadata"
    }
  }
}
