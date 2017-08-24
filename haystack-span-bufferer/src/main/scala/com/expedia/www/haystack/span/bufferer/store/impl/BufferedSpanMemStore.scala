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
import java.util.concurrent.atomic.AtomicInteger

import com.codahale.metrics.Meter
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.span.bufferer.metrics.AppMetricNames._
import com.expedia.www.haystack.span.bufferer.metrics.MetricsSupport
import com.expedia.www.haystack.span.bufferer.serde.SpanBufferSerde
import com.expedia.www.haystack.span.bufferer.store.DynamicCacheSizer
import com.expedia.www.haystack.span.bufferer.store.data.model.BufferedSpanWithMetadata
import com.expedia.www.haystack.span.bufferer.store.traits.{BufferedSpanKVStore, CacheSizeObserver, EldestBufferedSpanEvictionListener}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.internals.ProcessorStateManager
import org.apache.kafka.streams.processor.{ProcessorContext, StateRestoreCallback, StateStore}
import org.apache.kafka.streams.state.{KeyValueIterator, StateSerdes}

import scala.collection.JavaConversions._
import scala.collection.mutable

object BufferedSpanMemStore extends MetricsSupport {
  protected val evictionMeter: Meter = metricRegistry.meter(STATE_STORE_EVICTION)
}

class BufferedSpanMemStore(val name: String, cacheSizer: DynamicCacheSizer) extends BufferedSpanKVStore with CacheSizeObserver {

  @volatile protected var open = false
  protected var serdes: StateSerdes[String, SpanBuffer] = _

  protected val maxEntries = new AtomicInteger(cacheSizer.defaultSizePerCache)

  private val listeners: mutable.ListBuffer[EldestBufferedSpanEvictionListener] = mutable.ListBuffer()

  // initialize the map
  protected val map = new util.LinkedHashMap[String, BufferedSpanWithMetadata](cacheSizer.defaultSizePerCache, 1.01f, false) {
    override protected def removeEldestEntry(eldest: util.Map.Entry[String, BufferedSpanWithMetadata]): Boolean = {
      if (size >= maxEntries.get()) {
        BufferedSpanMemStore.evictionMeter.mark()
        listeners.foreach(listener => listener.onEvict(eldest.getKey, eldest.getValue))
        true
      } else {
        false
      }
    }
  }

  /**
    * Initializes the state store
    * @param context processor context
    * @param root root state store
    */
  override def init(context: ProcessorContext, root: StateStore): Unit = {
    cacheSizer.addCacheObserver(this)

    val storeChangelogTopic = ProcessorStateManager.storeChangelogTopic(context.applicationId, name)

    // construct the serde for the state manager
    this.serdes = new StateSerdes[String, SpanBuffer](storeChangelogTopic, Serdes.String(), new SpanBufferSerde())

    // register the store
    context.register(root, true, new StateRestoreCallback() {
      override def restore(key: Array[Byte], value: Array[Byte]): Unit = {
        if (value == null) {
          map.remove(serdes.keyFrom(key))
        } else {
          // restore the span-buffer object with Long.MinValue as the first recorded timestamp
          // this makes sure that all these restored buffers will be collected and emitted out in the first
          // punctuate call.
          map.put(serdes.keyFrom(key), BufferedSpanWithMetadata(serdes.valueFrom(value).toBuilder, Long.MinValue))
        }
      }
    })

    open = true
  }

  /**
    * removes and returns all the span buffers from the map that are recorded before the given timestamp
    * @param timestamp timestamp before which all buffered spans should be read and removed
    * @return
    */
  override def getAndRemoveSpansOlderThan(timestamp: Long): util.Map[String, BufferedSpanWithMetadata] = {
    val result = new util.HashMap[String, BufferedSpanWithMetadata]()

    val iterator = this.map.entrySet().iterator()
    var done = false

    while (!done && iterator.hasNext) {
      val el = iterator.next()
      if (el.getValue.firstSpanSeenAt <= timestamp) {
        iterator.remove()
        result.put(el.getKey, el.getValue)
      } else {
        // here we apply a basic optimization and skip further iteration because all following records
        // in this map will have higher recordTimestamp. When we insert the first span for a unique traceId
        // in the map, we set the 'firstRecordTimestamp' attribute with record's timestamp
        done = true
      }
    }
    result
  }

  override def get(key: String): BufferedSpanWithMetadata = this.map.get(key)

  override def put(key: String, value: BufferedSpanWithMetadata): Unit = {
    this.map.put(key, value)
  }

  override def putIfAbsent(key: String, value: BufferedSpanWithMetadata): BufferedSpanWithMetadata = {
    this.map.putIfAbsent(key, value)
  }

  override def putAll(entries: util.List[KeyValue[String, BufferedSpanWithMetadata]]): Unit = {
    for (entry <- entries) put(entry.key, entry.value)
  }

  override def delete(key: String): BufferedSpanWithMetadata = this.map.remove(key)

  override def approximateNumEntries(): Long = this.map.size()

  override def addEvictionListener(l: EldestBufferedSpanEvictionListener): Unit = this.listeners += l

  /**
    * for an in-memory store, no flush operation is required
    */
  override def flush(): Unit = ()

  /**
    * this is an in-memory store
    * @return false
    */
  override def persistent(): Boolean = false

  override def close(): Unit = {
    cacheSizer.removeCacheObserver(this)
    open = false
  }

  override def isOpen: Boolean = open

  override def range(from: String, to: String): KeyValueIterator[String, BufferedSpanWithMetadata] = {
    throw new UnsupportedOperationException("BufferedSpanMemStore does not support range() function.")
  }

  override def all(): KeyValueIterator[String, BufferedSpanWithMetadata] = {
    throw new UnsupportedOperationException("BufferedSpanMemStore does not support all() function.")
  }

  def onCacheSizeChange(maxEntries: Int): Unit = this.maxEntries.set(maxEntries)
}
