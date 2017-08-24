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
package com.expedia.www.haystack.span.bufferer.store.impl

import java.util

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.span.bufferer.store.data.model.BufferedSpanWithMetadata
import com.expedia.www.haystack.span.bufferer.store.traits.EldestBufferedSpanEvictionListener
import com.expedia.www.haystack.span.bufferer.store.{BufferedSpanStoreChangeLogger, DynamicCacheSizer}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}

import scala.collection.JavaConversions._

class BufferedSpanLoggingEnabledMemStore(val storeName: String, dynamicCacheSizer: DynamicCacheSizer)
  extends BufferedSpanMemStore(storeName, dynamicCacheSizer) {

  private var changeLogger: BufferedSpanStoreChangeLogger[String, SpanBuffer] = _

  override def delete(key: String): BufferedSpanWithMetadata = {
    val result = super.delete(key)
    removed(key)
    result
  }

  override def put(key: String, value: BufferedSpanWithMetadata): Unit = {
    super.put(key, value)
    changeLogger.logChange(key, value.builder.build())
  }

  override def putAll(entries: util.List[KeyValue[String, BufferedSpanWithMetadata]]): Unit = {
    super.putAll(entries)
    entries.foreach {
      entry => changeLogger.logChange(entry.key, entry.value.builder.build())
    }
  }

  override def putIfAbsent(key: String, value: BufferedSpanWithMetadata): BufferedSpanWithMetadata = {
    val originalValue = super.putIfAbsent(key, value)
    if (originalValue == null) changeLogger.logChange(key, value.builder.build())
    originalValue
  }

  override def getAndRemoveSpansOlderThan(timestamp: Long): util.Map[String, BufferedSpanWithMetadata] = {
    val result = super.getAndRemoveSpansOlderThan(timestamp)
    result.keySet().foreach(removed)
    result
  }

  override def init(context: ProcessorContext, root: StateStore): Unit = {
    super.init(context, root)

    this.changeLogger = new BufferedSpanStoreChangeLogger[String, SpanBuffer](name, context, serdes)

    super.addEvictionListener(new EldestBufferedSpanEvictionListener {
      override def onEvict(key: String, value: BufferedSpanWithMetadata): Unit = removed(key)
    })

    open = true
  }

  /**
    * Called when the store removes an entry in response to a call from this
    * store.
    *
    * @param key the key for the entry that the inner store removed
    */
  protected def removed(key: String): Unit = {
    if (changeLogger != null) changeLogger.logChange(key, null)
  }
}
