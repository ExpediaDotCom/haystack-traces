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

package com.expedia.www.haystack.stitched.span.collector.config.reload

import java.util.concurrent.{Executors, TimeUnit}

import com.expedia.www.haystack.stitched.span.collector.config.entities.ReloadConfiguration

abstract class ConfigurationReloadProvider(config: ReloadConfiguration) extends AutoCloseable {

  private val executor = Executors.newSingleThreadScheduledExecutor()

  // schedule the reload process from an external store
  if(config.reloadIntervalInMillis > -1) {
    executor.scheduleWithFixedDelay(new Runnable() {
      override def run(): Unit = load()
    }, config.reloadIntervalInMillis, config.reloadIntervalInMillis, TimeUnit.MILLISECONDS)
  }

  def load(): Unit

  def close(): Unit = executor.shutdownNow()
}
