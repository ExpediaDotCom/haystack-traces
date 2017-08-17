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

package com.expedia.www.haystack.stitch.span.collector.config.entities

import com.expedia.www.haystack.stitch.span.collector.config.reload.Reloadable
import org.apache.commons.lang3.StringUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.slf4j.LoggerFactory

object IndexAttribute {
  val empty = IndexAttribute("", "", enabled = false)
}

case class IndexAttribute(name: String, `type`: String, enabled: Boolean)

case class IndexConfigDatabaseModel(serviceField: IndexAttribute,
                                    operationField: IndexAttribute,
                                    durationField: IndexAttribute,
                                    tags: List[IndexAttribute],
                                    logTags: List[IndexAttribute])

case class IndexConfiguration(var serviceField: IndexAttribute = IndexAttribute.empty,
                              var operationField: IndexAttribute = IndexAttribute.empty,
                              var durationField: IndexAttribute = IndexAttribute.empty,
                              var tags: Map[String, IndexAttribute] = Map.empty,
                              var logTags: Map[String, IndexAttribute] = Map.empty) extends Reloadable {

  private val LOGGER = LoggerFactory.getLogger(classOf[IndexConfiguration])

  implicit val formats = DefaultFormats
  private var currentVersion: Int = 0

  override val name: String = "indexing-fields"

  override def onReload(newConfigStr: String): Unit = {
    if(StringUtils.isNotEmpty(newConfigStr) && hasConfigChanged(newConfigStr)) {
      LOGGER.info("new indexing configuration has arrived: " + newConfigStr)
      val newConfig = Serialization.read[IndexConfigDatabaseModel](newConfigStr)
      update(newConfig)
      // set the current version to newer one
      currentVersion = newConfigStr.hashCode
    }
  }

  private def update(newConfig: IndexConfigDatabaseModel): Unit = {
    serviceField = newConfig.serviceField
    operationField = newConfig.operationField
    durationField = newConfig.durationField
    tags = if (newConfig.tags != null) newConfig.tags.groupBy(_.name).mapValues(_.head) else Map.empty
    logTags = if (newConfig.logTags != null) newConfig.logTags.groupBy(_.name).mapValues(_.head) else Map.empty
  }

  private def hasConfigChanged(newConfigStr: String): Boolean = newConfigStr.hashCode != currentVersion
}
