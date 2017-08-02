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

  private val ENV_NAME_PREFIX = "HAYSTACK_"

  /**
    * Load and return the configuration
    * Env variables take the highest priority, followed by overrides config file and the last being base.conf
    */
  lazy val loadAppConfig: Config = {

    val baseConfig = ConfigFactory.load("config/base.conf")

    sys.env.get("OVERRIDES_CONFIG_PATH") match {
      case Some(path) => ConfigFactory.load(path).withFallback(baseConfig)
      case _ => overrideConfigWithEnvVars(baseConfig)
    }
  }


  /**
    * @param config configuration object
    * @return new config object with configuration values overridden by haystack specific environment variables
    */
  private def overrideConfigWithEnvVars(config: Config): Config = {
    val result = new util.HashMap[String, Object]()

    config.entrySet().foreach(e => result.put(e.getKey, e.getValue.unwrapped()))

    sys.env
      .filter {
        case (propName, _) => isHaystackProperty(propName)
      }
      .map {
        case (propName, propValue) => (transformPropertyName(propName), propValue)
      }
      .foreach {
          case (propName, propValue) => result.put(propName, propValue)
      }

    ConfigFactory.parseMap(result)
  }

  private def isHaystackProperty(propName: String) = propName.startsWith(ENV_NAME_PREFIX)
  private def transformPropertyName(propName: String) = {
    propName
      .replaceFirst(ENV_NAME_PREFIX, "")
      .toLowerCase
      .replace("_", ".")
  }
}
