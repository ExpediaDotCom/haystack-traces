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
package com.expedia.www.haystack.span.stitcher.config

import java.util

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

object ConfigurationLoader {

  /**
    * Load and return the configuration
    * Env variables take the highest priority, followed by overrides config file and the last being base.conf
    */
  lazy val loadAppConfig: Config = {

    val baseConfigPath = ConfigFactory.load("config/base.conf")

    sys.env.get("OVERRIDES_CONFIG_PATH") match {
      case Some(path) => ConfigFactory.load(path).withFallback(baseConfigPath)
      case _ =>
        val baseConfig = ConfigFactory.load(baseConfigPath)
        overrideConfigWithEnvVars(baseConfig, "HAYSTACK")
    }
  }


  /**
    * @param config configuration object
    * @param envPrefixName search the environment variables with this prefix name and config key
    * @return new config with config' values overridden by environment variables(if present)
    */
  private def overrideConfigWithEnvVars(config: Config, envPrefixName: String): Config = {
    val result = new util.HashMap[String, Object]()

    config.entrySet().foreach(e => {
      val configKey = e.getKey
      val envName = envPrefixName + "_" + configKey.toUpperCase.replace(".", "_")

      sys.env.get(envName) match {
        case Some(envValue) => result.put(configKey, envValue)
        case _ => result.put(configKey, e.getValue.unwrapped())
      }
    })

    ConfigFactory.parseMap(result)
  }
}
