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

package com.expedia.www.haystack.trace.commons.config

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValueType}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object ConfigurationLoader {

  private val LOGGER = LoggerFactory.getLogger(ConfigurationLoader.getClass)

  private val ENV_NAME_PREFIX = "HAYSTACK_PROP_"

  /**
    * Load and return the configuration
    * if overrides_config_path env variable exists, then we load that config file and use base conf as fallback,
    * else we load the config from env variables(prefixed with haystack) and use base conf as fallback
    *
    */
  lazy val loadAppConfig: Config = {
    val baseConfig = ConfigFactory.load("config/base.conf")

    val keysWithArrayValues = baseConfig.entrySet()
      .filter(_.getValue.valueType() == ConfigValueType.LIST)
      .map(_.getKey)
      .toSet

    val config = sys.env.get("HAYSTACK_OVERRIDES_CONFIG_PATH") match {
      case Some(path) => ConfigFactory.parseFile(new File(path)).withFallback(baseConfig).resolve()
      case _ => loadFromEnvVars(keysWithArrayValues).withFallback(baseConfig).resolve()
    }

    LOGGER.info(config.root().render())
    
    config
  }

  /**
    * @return new config object with haystack specific environment variables
    */
  private def loadFromEnvVars(keysWithArrayValues: Set[String]): Config = {
    val envMap: Map[String, Object] = sys.env.filter {
      case (envName, _) => isHaystackEnvVar(envName)
    } map {
      case (envName, envValue) =>
        val key = transformEnvVarName(envName)
        if (keysWithArrayValues.contains(key)) (key, transformEnvVarArrayValue(envValue)) else (key, envValue)
    }

    ConfigFactory.parseMap(envMap)
  }

  private def isHaystackEnvVar(env: String): Boolean = env.startsWith(ENV_NAME_PREFIX)

  /**
    * converts the env variable to HOCON format
    * for e.g. env variable HAYSTACK_KAFKA_STREAMS_NUM_STREAM_THREADS gets converted to kafka.streams.num.stream.threads
    * @param env environment variable name
    * @return variable name that complies with hocon key
    */
  private def transformEnvVarName(env: String): String = {
    env.replaceFirst(ENV_NAME_PREFIX, "").toLowerCase.replace("_", ".")
  }

  /**
    * converts the env variable value to iterable object if it starts and ends with '[' and ']' respectively.
    * @param env environment variable value
    * @return string or iterable object
    */
  private def transformEnvVarArrayValue(env: String): java.util.List[String] = {
    if (env.startsWith("[") && env.endsWith("]")) {
      import scala.collection.JavaConverters._
      env.substring(1, env.length - 1).split(',').filter(StringUtils.isNotEmpty).toList.asJava
    } else {
      throw new RuntimeException("config key is of array type, so it should start and end with '[', ']' respectively")
    }
  }
}
