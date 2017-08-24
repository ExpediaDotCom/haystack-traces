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
package com.expedia.www.haystack.span.bufferer.store.traits

import com.expedia.www.haystack.span.bufferer.store.data.model.BufferedSpanWithMetadata

/**
  * the listener is called when the eldest buffered span is evicted from the cache
  */
trait EldestBufferedSpanEvictionListener {
  def onEvict(key: String, value: BufferedSpanWithMetadata): Unit
}
