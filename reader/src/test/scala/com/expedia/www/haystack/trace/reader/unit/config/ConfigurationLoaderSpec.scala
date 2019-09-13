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
      val readerPort: Int = if (System.getenv("READERPORT") == null) 8088 else Integer.parseInt(System.getenv("READERPORT"))

      serviceConfig.port shouldBe readerPort
      serviceConfig.ssl.enabled shouldBe false
      serviceConfig.ssl.certChainFilePath shouldBe "/ssl/cert"
      serviceConfig.ssl.privateKeyPath shouldBe "/ssl/private-key"
      serviceConfig.maxSizeInBytes shouldBe 52428800
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

      val ELASTICSEARCH_HOST = if (System.getenv("ELASTICSEARCH_HOST") == null) "elasticsearch" else System.getenv("ELASTICSEARCH_HOST")
      val ELASTIC_SEARCH_ENDPOINT = "http://"+ELASTICSEARCH_HOST+":9200"
      elasticSearchConfig.clientConfiguration.endpoint shouldEqual ELASTIC_SEARCH_ENDPOINT
      elasticSearchConfig.clientConfiguration.connectionTimeoutMillis shouldEqual 10000
      elasticSearchConfig.clientConfiguration.readTimeoutMillis shouldEqual 5000


      elasticSearchConfig.spansIndexConfiguration.indexHourBucket shouldEqual 6
      elasticSearchConfig.spansIndexConfiguration.indexHourTtl shouldEqual 72
      elasticSearchConfig.spansIndexConfiguration.useRootDocumentStartTime shouldEqual true
      elasticSearchConfig.spansIndexConfiguration.indexType shouldEqual "spans"
      elasticSearchConfig.spansIndexConfiguration.indexNamePrefix shouldEqual "haystack-traces"


      elasticSearchConfig.serviceMetadataIndexConfiguration.enabled shouldEqual false
      elasticSearchConfig.serviceMetadataIndexConfiguration.indexName shouldEqual "service_metadata"
      elasticSearchConfig.serviceMetadataIndexConfiguration.indexType shouldEqual "metadata"
    }

    it("should load trace backend configuration") {
      val traceBackendConfig = new ProviderConfiguration().traceBackendConfiguration
      traceBackendConfig.backends.head.host shouldEqual "localhost"
      traceBackendConfig.backends.head.port shouldEqual 8090
    }
  }
}
