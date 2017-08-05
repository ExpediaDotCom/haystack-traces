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
package com.expedia.www.haystack.span.stitcher.store.traits

import com.expedia.www.haystack.span.stitcher.store.data.model.StitchedSpanWithMetadata

/**
  * the listener is called when the eldest stitched span is removed from the cache
  */
trait EldestStitchedSpanRemovalListener {
  def onRemove(key: String, value: StitchedSpanWithMetadata): Unit
}
