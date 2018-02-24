package com.expedia.www.haystack.trace.commons.unit

import com.expedia.www.haystack.trace.commons.config.ConfigurationLoader
import org.scalatest.{FunSpec, Matchers}

class ConfigurationLoaderSpec extends FunSpec with Matchers {

  describe("configuration loader") {
    val keyName = "traces.key.sequence"

    it ("should load config from env variable with empty array value") {
      val envVars = Map[String, String](ConfigurationLoader.ENV_NAME_PREFIX + "TRACES_KEY_SEQUENCE" -> "[]")
      val config = ConfigurationLoader.loadFromEnv(envVars, Set(keyName))
      config.getList(keyName).size() shouldBe 0
    }

    it ("should load config from env variable with non-empty array value") {
      val envVars = Map[String, String](ConfigurationLoader.ENV_NAME_PREFIX + "TRACES_KEY_SEQUENCE" -> "[v1]")
      val config = ConfigurationLoader.loadFromEnv(envVars, Set(keyName))
      config.getStringList(keyName).size() shouldBe 1
      config.getStringList(keyName).get(0) shouldBe "v1"
    }

    it ("should throw runtime exception if env variable doesn't comply array value signature - [..]") {
      val envVars = Map[String, String](ConfigurationLoader.ENV_NAME_PREFIX + "TRACES_KEY_SEQUENCE" -> "v1")
      val exception = intercept[RuntimeException] {
        ConfigurationLoader.loadFromEnv(envVars, Set(keyName))
      }

      exception.getMessage shouldEqual "config key is of array type, so it should start and end with '[', ']' respectively"
    }

    it ("should load config from env variable with non-empty value") {
      val envVars = Map[String, String](
        ConfigurationLoader.ENV_NAME_PREFIX + "TRACES_KEY_SEQUENCE" -> "[v1]",
        ConfigurationLoader.ENV_NAME_PREFIX + "TRACES_KEY2" -> "v2",
        "NON_HAYSTACK_KEY" -> "not_interested")

      val config = ConfigurationLoader.loadFromEnv(envVars, Set(keyName))
      config.getStringList(keyName).size() shouldBe 1
      config.getStringList(keyName).get(0) shouldBe "v1"
      config.getString("traces.key2") shouldBe "v2"
      config.hasPath("non.haystack.key") shouldBe false
    }
  }
}
