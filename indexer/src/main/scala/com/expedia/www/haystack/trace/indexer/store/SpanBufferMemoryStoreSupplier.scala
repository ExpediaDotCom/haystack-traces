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

package com.expedia.www.haystack.trace.indexer.store

import java.util

import com.expedia.www.haystack.trace.indexer.store.impl.{SpanBufferLoggingEnabledBufferMemoryStore, SpanBufferMemoryStore}
import com.expedia.www.haystack.trace.indexer.store.traits.SpanBufferKeyValueStore
import org.apache.kafka.streams.processor.StateStoreSupplier

class SpanBufferMemoryStoreSupplier(minTracesPerCache: Int,
                                    maxEntriesAcrossStores: Int,
                                    val name: String,
                                    val loggingEnabled: Boolean,
                                    val logConfig: util.Map[String, String])
  extends StateStoreSupplier[SpanBufferKeyValueStore] {

  private val dynamicCacheSizer = new DynamicCacheSizer(minTracesPerCache, maxEntriesAcrossStores)

  /**
    * @return kv store for maintaining buffered-spans. If logging is enabled, we persist the changelog to kafka topic
    *         else it is purely in-memory
    */
  override def get(): SpanBufferKeyValueStore = {
    if(loggingEnabled) {
      new SpanBufferLoggingEnabledBufferMemoryStore(name, dynamicCacheSizer)
    } else {
      new SpanBufferMemoryStore(name, dynamicCacheSizer)
    }
  }
}
