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

import java.util

import com.expedia.www.haystack.span.stitcher.store.data.model.StitchedSpanWithMetadata
import org.apache.kafka.streams.state.KeyValueStore

/**
  * this interface extends KeyValueStore to provide stitch span operations
  */
trait StitchedSpanKVStore extends KeyValueStore[String, StitchedSpanWithMetadata] {

  /**
    * get all stitched span objects that are recorded before the stitch-window timestamp.
    * Stitch window timestamp is measured as (currentTimeMs - stitchWindowMillis)
    * @param stitchWindowMillis stitch window in millis
    * @return
    */
  def getAndRemoveSpansInWindow(stitchWindowMillis: Long): util.Map[String, StitchedSpanWithMetadata]

  /**
    * add a listener to the store, that gets called when the eldest stitched span object is evicted
    * due to constraints of maxEntries in the store cache
    * @param l listener object that is called by the store
    */
  def addEvictionListener(l: EldestStitchedSpanEvictionListener): Unit
}
