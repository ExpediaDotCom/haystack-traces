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

package com.expedia.www.haystack.span.stitcher.store.impl

import java.util

import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.span.stitcher.store.StitchedSpanStoreChangeLogger
import com.expedia.www.haystack.span.stitcher.store.data.model.StitchedSpanWithMetadata
import com.expedia.www.haystack.span.stitcher.store.traits.EldestStitchedSpanRemovalListener
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.internals.ProcessorStateManager
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}

import scala.collection.JavaConversions._

class StitchedSpanLoggingEnabledMemStore(val storeName: String, maxEntries: Int) extends StitchedSpanMemStore(storeName, maxEntries) {

  private var changeLogger: StitchedSpanStoreChangeLogger = _

  override def delete(key: Array[Byte]): StitchedSpanWithMetadata = {
    val result = super.delete(key)
    removed(key)
    result
  }

  override def put(key: Array[Byte], value: StitchedSpanWithMetadata): Unit = {
    super.put(key, value)
    changeLogger.logChange(key, value.builder.build())
  }

  override def putAll(entries: util.List[KeyValue[Array[Byte], StitchedSpanWithMetadata]]): Unit = {
    super.putAll(entries)
    entries.foreach {
      entry => changeLogger.logChange(entry.key, entry.value.builder.build())
    }
  }

  override def putIfAbsent(key: Array[Byte], value: StitchedSpanWithMetadata): StitchedSpanWithMetadata = {
    val originalValue = super.putIfAbsent(key, value)
    if (originalValue == null) changeLogger.logChange(key, value.builder.build())
    originalValue
  }

  override def getAndRemoveSpansInWindow(stitchWindowMillis: Long): util.Map[Array[Byte], StitchedSpanWithMetadata] = {
    val result = super.getAndRemoveSpansInWindow(stitchWindowMillis)
    result.keySet().foreach(removed)
    result
  }

  override def get(key: Array[Byte]): StitchedSpanWithMetadata = super.get(key)

  override def approximateNumEntries(): Long = super.approximateNumEntries()

  override def init(context: ProcessorContext, root: StateStore): Unit = {
    super.init(context, root)

    // construct the serde for the state manager
    val changeLogTopic = ProcessorStateManager.storeChangelogTopic(context.applicationId, name)

    this.changeLogger = new StitchedSpanStoreChangeLogger(name, context, serdes)

    super.addRemovalListener(new EldestStitchedSpanRemovalListener {
      override def onRemove(key: Array[Byte], value: StitchedSpanWithMetadata): Unit = removed(key)
    })

    open = true
  }

    override def addRemovalListener(l: EldestStitchedSpanRemovalListener): Unit = super.addRemovalListener(l)

  /**
    * Called when the underlying {@link #innerStore} {@link StitchedSpanMemStore} removes an entry in response to a call from this
    * store.
    *
    * @param key the key for the entry that the inner store removed
    */
  protected def removed(key: Array[Byte]): Unit = {
    changeLogger.logChange(key, null)
  }

  override def getRestoredStateIterator(): util.Iterator[(Array[Byte], StitchedSpan)] = {
    super.getRestoredStateIterator()
  }

  override def clearRestoredState(): Unit = super.clearRestoredState()
}
