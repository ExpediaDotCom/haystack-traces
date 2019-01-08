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
package com.expedia.www.haystack.trace.storage.backends.mysql.unit.config

import com.expedia.www.haystack.trace.storage.backends.mysql.config.ProjectConfiguration
import com.expedia.www.haystack.trace.storage.backends.mysql.config.entities.ServiceConfiguration
import com.expedia.www.haystack.trace.storage.backends.mysql.unit.BaseUnitTestSpec

class ConfigurationLoaderSpec extends BaseUnitTestSpec {
  describe("ConfigurationLoader") {
    val project = new ProjectConfiguration()
    it("should load the service config from base.conf") {
      val serviceConfig: ServiceConfiguration = project.serviceConfig
      serviceConfig.port shouldBe 8090
      serviceConfig.ssl.enabled shouldBe true
      serviceConfig.ssl.certChainFilePath shouldBe "/ssl/cert"
      serviceConfig.ssl.privateKeyPath shouldBe "/ssl/private-key"
    }
    it("should load the mysql config from base.conf and few properties overridden from env variable") {
      val mysqlConfig = project.mysqlConfig
      val clientConfig = mysqlConfig.clientConfig

      // this will fail if run inside an editor, we override this config using env variable inside pom.xml
      clientConfig.url shouldBe "mysql"
      clientConfig.spansTable.autoCreateSchema shouldBe Some("mysql_table_schema")
      clientConfig.spansTable.name shouldBe "spans"
      clientConfig.spansTable.recordTTLInSec shouldBe 86400

      clientConfig.socket.keepAlive shouldBe true
      clientConfig.socket.maxConnectionPerHost shouldBe 100
      clientConfig.socket.readTimeoutMills shouldBe 5000
      clientConfig.socket.connectionTimeoutMillis shouldBe 10000
      mysqlConfig.retryConfig.maxRetries shouldBe 10
      mysqlConfig.retryConfig.backOffInMillis shouldBe 100
      mysqlConfig.retryConfig.backoffFactor shouldBe 2    }

  }
}
