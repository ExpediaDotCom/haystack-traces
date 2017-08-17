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

case class IndexAttribute(name: String, `type`: String)

case class IndexConfiguration(var serviceFieldName: String = "",
                              var operationFieldName: String = "",
                              var durationFieldName: String = "",
                              var indexableTags: Map[String, IndexAttribute] = Map()) extends Reloadable {

  override val name: String = "indexingFields"
  override def onReload(newConfig: String): Unit = {
    // update the tagKeys with this new config
  }
}
