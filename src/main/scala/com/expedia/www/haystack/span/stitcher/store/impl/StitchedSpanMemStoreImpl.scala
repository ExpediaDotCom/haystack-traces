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
import com.expedia.www.haystack.span.stitcher.serde.StitchedSpanSerde
import com.expedia.www.haystack.span.stitcher.store.StitchedSpanMemStore
import com.expedia.www.haystack.span.stitcher.store.data.model.StitchedSpanWithMetadata
import com.expedia.www.haystack.span.stitcher.store.traits.EldestStitchedSpanRemovalListener
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.internals.ProcessorStateManager
import org.apache.kafka.streams.processor.{ProcessorContext, StateRestoreCallback, StateStore}
import org.apache.kafka.streams.state.StateSerdes

import scala.collection.JavaConversions._
import scala.collection.mutable

class StitchedSpanMemStoreImpl(val name: String,
                               maxEntries: Int) extends StitchedSpanMemStore {

  private val listeners: mutable.ListBuffer[EldestStitchedSpanRemovalListener] = mutable.ListBuffer()

  private var serdes: StateSerdes[Array[Byte], StitchedSpan] = _

  // initialize the restored store
  private var restoredState = new util.HashMap[Array[Byte], StitchedSpan]()

  // initialize the map
  protected val map = new util.LinkedHashMap[Array[Byte], StitchedSpanWithMetadata](maxEntries + 1, 1.01f, false) {
    override protected def removeEldestEntry(eldest: util.Map.Entry[Array[Byte], StitchedSpanWithMetadata]): Boolean = {
      if (size > maxEntries) {
        listeners.foreach(listener => listener.onRemove(eldest.getKey, eldest.getValue))
        return true
      }
      false
    }
  }

  /**
    * Initializes the state store
    * @param context processor context
    * @param root root state store
    */
  override def init(context: ProcessorContext, root: StateStore): Unit = {
    val storeChangelogTopic = ProcessorStateManager.storeChangelogTopic(context.applicationId, name)

    // construct the serde for the state manager
    this.serdes = new StateSerdes[Array[Byte], StitchedSpan](storeChangelogTopic, Serdes.ByteArray(), StitchedSpanSerde)

    // register the store
    context.register(root, true, new StateRestoreCallback() {
      override def restore(key: Array[Byte], value: Array[Byte]): Unit = { // check value for null, to avoid  deserialization error.
        if (value == null) restoreState(serdes.keyFrom(key), null)
        else restoreState(serdes.keyFrom(key), serdes.valueFrom(value))
      }
    })

    open = true
  }

  private def restoreState(key: Array[Byte], stitchedSpan: StitchedSpan): Unit = this.restoredState.put(key, stitchedSpan)

  /**
    * removes and returns all the stitched span objects from the map that have the timestamp less than current time
    * minus stitch-window interval
    * @param stitchWindowMillis stitch window interval in millis
    * @return
    */
  override def getAndRemoveSpansInWindow(stitchWindowMillis: Long): util.Map[Array[Byte], StitchedSpanWithMetadata] = {
    val result = new util.HashMap[Array[Byte], StitchedSpanWithMetadata]()

    val iterator = this.map.entrySet().iterator()
    var done = false

    while (!done && iterator.hasNext) {
      val el = iterator.next()
      if (el.getValue.firstRecordTimestamp + stitchWindowMillis <= System.currentTimeMillis()) {
        iterator.remove()
        result.put(el.getKey, el.getValue)
      } else {
        // here we apply a basic optimization and skip further iteration because all following records
        // in this map will have higher recordTimestamp. When we insert the first span for a unique traceId
        // in the map, we set the 'firstRecordTimestamp' attribute as System.currentTimeMillis
        done = true
      }
    }
    result
  }

  override def get(key: Array[Byte]): StitchedSpanWithMetadata = this.map.get(key)

  override def put(key: Array[Byte], value: StitchedSpanWithMetadata): Unit = {
    this.map.put(key, value)
  }

  override def putIfAbsent(key: Array[Byte], value: StitchedSpanWithMetadata): StitchedSpanWithMetadata = {
    this.map.putIfAbsent(key, value)
  }

  override def putAll(entries: util.List[KeyValue[Array[Byte], StitchedSpanWithMetadata]]): Unit = {
    for (entry <- entries) put(entry.key, entry.value)
  }

  override def delete(key: Array[Byte]): StitchedSpanWithMetadata = this.map.remove(key)

  override def approximateNumEntries(): Long = this.map.size()

  override def addRemovalListener(l: EldestStitchedSpanRemovalListener): Unit = this.listeners += l

  override def getRestoredStateIterator(): util.Iterator[(Array[Byte], StitchedSpan)] = this.restoredState.iterator

  override def clearRestoredState(): Unit = {
    if (!this.restoredState.isEmpty) {
      this.restoredState = new util.HashMap[Array[Byte], StitchedSpan]()
    }
  }
}
