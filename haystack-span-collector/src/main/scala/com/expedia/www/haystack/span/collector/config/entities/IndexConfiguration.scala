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

package com.expedia.www.haystack.span.collector.config.entities

import com.expedia.www.haystack.span.collector.config.reload.Reloadable
import org.apache.commons.lang3.StringUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.slf4j.LoggerFactory

case class IndexField(name: String, `type`: String, enabled: Boolean = true)

case class IndexConfiguration(var indexableTags: List[IndexField] = Nil) extends Reloadable {
  var indexableTagsByTagName: Map[String, IndexField] = groupTagsWithKey(indexableTags)

  private val LOGGER = LoggerFactory.getLogger(classOf[IndexConfiguration])

  implicit val formats = DefaultFormats
  private var currentVersion: Int = 0
  var reloadConfigTableName: String = ""

  override def name: String = reloadConfigTableName

  override def onReload(newConfigStr: String): Unit = {
    if(StringUtils.isNotEmpty(newConfigStr) && hasConfigChanged(newConfigStr)) {
      LOGGER.info("new indexing configuration has arrived: " + newConfigStr)
      val newConfig = Serialization.read[IndexConfiguration](newConfigStr)
      update(newConfig)
      // set the current version to newer one
      currentVersion = newConfigStr.hashCode
    }
  }

  private def update(newConfig: IndexConfiguration): Unit = {
     if (newConfig.indexableTags != null) {
       this.indexableTags = newConfig.indexableTags
       this.indexableTagsByTagName = groupTagsWithKey(this.indexableTags)
    }
  }

  private def groupTagsWithKey(indexableTags: List[IndexField]): Map[String, IndexField] = {
    indexableTags.groupBy(_.name).mapValues(_.head)
  }
  private def hasConfigChanged(newConfigStr: String): Boolean = newConfigStr.hashCode != currentVersion
}
