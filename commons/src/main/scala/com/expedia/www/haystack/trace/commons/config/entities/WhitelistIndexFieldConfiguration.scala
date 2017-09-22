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

package com.expedia.www.haystack.trace.commons.config.entities

import java.util.concurrent.atomic.AtomicReference

import com.expedia.www.haystack.trace.commons.config.reload.Reloadable
import org.apache.commons.lang3.StringUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.slf4j.LoggerFactory

case class WhitelistIndexField(name: String, `type`: String, enabled: Boolean = true)

case class WhitelistIndexFieldConfiguration(var indexableTags: List[WhitelistIndexField] = Nil) extends Reloadable {

  private val LOGGER = LoggerFactory.getLogger(classOf[WhitelistIndexFieldConfiguration])
  private var currentVersion: Int = 0
  implicit val formats = DefaultFormats

  val indexableTagsByTagName: AtomicReference[Map[String, WhitelistIndexField]] = new AtomicReference[Map[String, WhitelistIndexField]]()

  groupTagsWithKey()

  var reloadConfigTableName: Option[String] = None

  // fail fast
  override def name: String = reloadConfigTableName
    .getOrElse(throw new RuntimeException("fail to find the reload config table name!"))

  /**
    * this is called whenever the configuration reloader system reads the configuration object from external store
    * we check if the config data has changed using the string's hashCode
    * @param configData config object that is loaded at regular intervals from external store
    */
  override def onReload(configData: String): Unit = {
    if(StringUtils.isNotEmpty(configData) && hasConfigChanged(configData)) {
      LOGGER.info("new indexing configuration has arrived: " + configData)
      val newConfig = Serialization.read[WhitelistIndexFieldConfiguration](configData)
      update(newConfig)
      // set the current version to newer one
      currentVersion = configData.hashCode
    }
  }

  /**
    * update the new index configuration
    * @param newConfig new config object
    */
  private def update(newConfig: WhitelistIndexFieldConfiguration): Unit = {
     if (newConfig.indexableTags != null) {
       this.indexableTags = newConfig.indexableTags
       groupTagsWithKey()
    }
  }

  /**
    * convert the list of tags as key value pair, key being the indexField name and value is indexField itself
    * @return
    */
  private def groupTagsWithKey(): Unit = {
    indexableTagsByTagName.set(indexableTags.groupBy(_.name).mapValues(_.head))
  }

  /**
    * detect if configuration has changed using the hashCode as version
    * @param newConfigData new configuration data
    * @return
    */
  private def hasConfigChanged(newConfigData: String): Boolean = newConfigData.hashCode != currentVersion
}
