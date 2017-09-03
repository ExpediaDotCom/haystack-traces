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
package com.expedia.www.haystack.trace.indexer.store.impl

import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.codahale.metrics.Meter
import com.expedia.open.tracing.Span
import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.indexer.metrics.AppMetricNames._
import com.expedia.www.haystack.trace.indexer.metrics.MetricsSupport
import com.expedia.www.haystack.trace.indexer.serde.SpanBufferSerde
import com.expedia.www.haystack.trace.indexer.store.DynamicCacheSizer
import com.expedia.www.haystack.trace.indexer.store.data.model.SpanBufferWithMetadata
import com.expedia.www.haystack.trace.indexer.store.traits.{CacheSizeObserver, EldestBufferedSpanEvictionListener, SpanBufferKeyValueStore}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.internals.ProcessorStateManager
import org.apache.kafka.streams.processor.{ProcessorContext, StateRestoreCallback, StateStore}
import org.apache.kafka.streams.state.{KeyValueIterator, StateSerdes}

import scala.collection.JavaConversions._
import scala.collection.mutable

object SpanBufferMemoryStore extends MetricsSupport {
  protected val evictionMeter: Meter = metricRegistry.meter(STATE_STORE_EVICTION)
}

class SpanBufferMemoryStore(val name: String, cacheSizer: DynamicCacheSizer) extends SpanBufferKeyValueStore with CacheSizeObserver {

  @volatile protected var open = false
  protected var serdes: StateSerdes[String, SpanBuffer] = _

  // This maxEntries will be adjusted by the dynamic cacheSizer, lets default it to a reasonable value 10000
  protected val maxEntries = new AtomicInteger(10000)

  private val listeners: mutable.ListBuffer[EldestBufferedSpanEvictionListener] = mutable.ListBuffer()
  private var restoring = false
  private var totalSpansInMemStore: Int = 0

  // initialize the map
  protected val map = new util.LinkedHashMap[String, SpanBufferWithMetadata](cacheSizer.minTracesPerCache, 1.01f, false) {
    override protected def removeEldestEntry(eldest: util.Map.Entry[String, SpanBufferWithMetadata]): Boolean = {
      val evict = totalSpansInMemStore >= maxEntries.get()
      if (evict) {
        SpanBufferMemoryStore.evictionMeter.mark()
        totalSpansInMemStore -= eldest.getValue.builder.getChildSpansCount

        // dont apply changelog if in restore phase
        if(!restoring) listeners.foreach(listener => listener.onEvict(eldest.getKey, eldest.getValue))
      }
      evict
    }
  }

  /**
    * Initializes the state store
    *
    * @param context processor context
    * @param root    root state store
    */
  override def init(context: ProcessorContext, root: StateStore): Unit = {
    cacheSizer.addCacheObserver(this)

    val storeChangelogTopic = ProcessorStateManager.storeChangelogTopic(context.applicationId, name)

    // construct the serde for the state manager
    this.serdes = new StateSerdes[String, SpanBuffer](storeChangelogTopic, Serdes.String(), new SpanBufferSerde())

    // register the store
    context.register(root, true, new StateRestoreCallback() {
      override def restore(key: Array[Byte], value: Array[Byte]): Unit = {
        restoring = true
        if (value == null) {
          val result = map.remove(serdes.keyFrom(key))
          if (result != null) totalSpansInMemStore -= result.builder.getChildSpansCount
        } else {
          // restore the span-buffer object with Long.MinValue as the first recorded timestamp
          // this makes sure that all these restored buffers will be collected and emitted out in the first
          // punctuate call.
          val record = SpanBufferWithMetadata(serdes.valueFrom(value).toBuilder, Long.MinValue)
          _put(serdes.keyFrom(key), record)
        }
        restoring = false
      }
    })

    open = true
  }

  /**
    * removes and returns all the span buffers from the map that are recorded before the given timestamp
    *
    * @param timestamp timestamp before which all buffered spans should be read and removed
    * @return
    */
  override def getAndRemoveSpanBuffersOlderThan(timestamp: Long): util.Map[String, SpanBufferWithMetadata] = {
    val result = new util.HashMap[String, SpanBufferWithMetadata]()

    val iterator = this.map.entrySet().iterator()
    var done = false

    while (!done && iterator.hasNext) {
      val el = iterator.next()
      if (el.getValue.firstSpanSeenAt <= timestamp) {
        iterator.remove()
        totalSpansInMemStore -= el.getValue.builder.getChildSpansCount
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

  override def get(traceId: String): SpanBufferWithMetadata = this.map.get(traceId)

  override def put(traceId: String, value: SpanBufferWithMetadata): Unit = {
    _put(traceId, value)
  }

  override def putIfAbsent(traceId: String, value: SpanBufferWithMetadata): SpanBufferWithMetadata = {
    val existingValue = this.map.get(traceId)
    if (existingValue == null) _put(traceId, value)
    existingValue
  }

  override def putAll(entries: util.List[KeyValue[String, SpanBufferWithMetadata]]): Unit = {
    for (entry <- entries) _put(entry.key, entry.value)
  }

  override def delete(traceId: String): SpanBufferWithMetadata = {
    val value = this.map.remove(traceId)
    if (value != null) totalSpansInMemStore -= value.builder.getChildSpansCount
    value
  }

  override def approximateNumEntries(): Long = this.map.size()

  override def addEvictionListener(l: EldestBufferedSpanEvictionListener): Unit = this.listeners += l

  /**
    * for an in-memory store, no flush operation is required
    */
  override def flush(): Unit = ()

  /**
    * this is an in-memory store
    *
    * @return false
    */
  override def persistent(): Boolean = false

  override def close(): Unit = {
    cacheSizer.removeCacheObserver(this)
    open = false
  }

  override def isOpen: Boolean = open

  override def range(from: String, to: String): KeyValueIterator[String, SpanBufferWithMetadata] = {
    throw new UnsupportedOperationException("SpanBufferMemoryStore does not support range() function.")
  }

  override def all(): KeyValueIterator[String, SpanBufferWithMetadata] = {
    throw new UnsupportedOperationException("SpanBufferMemoryStore does not support all() function.")
  }

  def onCacheSizeChange(maxEntries: Int): Unit = this.maxEntries.set(maxEntries)

  protected def _put(traceId: String, value: SpanBufferWithMetadata): Unit = {
    val existingValue = get(traceId)
    if (existingValue == null) {
      totalSpansInMemStore += value.builder.getChildSpansCount
    } else {
      totalSpansInMemStore += (value.builder.getChildSpansCount - existingValue.builder.getChildSpansCount)
    }
    this.map.put(traceId, value)
  }

  override def addOrUpdateSpanBuffer(traceId: String, span: Span, spanRecordTimestamp: Long): SpanBufferWithMetadata = {
    var value = get(traceId)
    if (value == null) {
      val spanBuffer = SpanBuffer.newBuilder().setTraceId(span.getTraceId).addChildSpans(span)
      value = SpanBufferWithMetadata(spanBuffer, spanRecordTimestamp)
      this.map.put(traceId, value)
    } else {
      value.builder.addChildSpans(span)
    }
    totalSpansInMemStore += 1
    value
  }

  def totalSpans: Int = totalSpansInMemStore
}
