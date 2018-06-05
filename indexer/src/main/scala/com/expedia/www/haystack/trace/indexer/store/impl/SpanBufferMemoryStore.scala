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
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trace.indexer.metrics.AppMetricNames._
import com.expedia.www.haystack.trace.indexer.store.DynamicCacheSizer
import com.expedia.www.haystack.trace.indexer.store.data.model.SpanBufferWithMetadata
import com.expedia.www.haystack.trace.indexer.store.traits.{CacheSizeObserver, EldestBufferedSpanEvictionListener, SpanBufferKeyValueStore}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object SpanBufferMemoryStore extends MetricsSupport {
  protected val LOGGER: Logger = LoggerFactory.getLogger(SpanBufferMemoryStore.getClass)
  protected val evictionMeter: Meter = metricRegistry.meter(STATE_STORE_EVICTION)
}

class SpanBufferMemoryStore(cacheSizer: DynamicCacheSizer) extends SpanBufferKeyValueStore with CacheSizeObserver {

  import SpanBufferMemoryStore._

  @volatile protected var open = false

  // This maxEntries will be adjusted by the dynamic cacheSizer, lets default it to a reasonable value 10000
  protected val maxEntries = new AtomicInteger(10000)
  private val listeners: mutable.ListBuffer[EldestBufferedSpanEvictionListener] = mutable.ListBuffer()
  private var totalSpansInMemStore: Int = 0
  private var map: util.LinkedHashMap[String, SpanBufferWithMetadata] = _

  override def init() {
    cacheSizer.addCacheObserver(this)

    // initialize the map
    map = new util.LinkedHashMap[String, SpanBufferWithMetadata](cacheSizer.minTracesPerCache, 1.01f, false) {
      override protected def removeEldestEntry(eldest: util.Map.Entry[String, SpanBufferWithMetadata]): Boolean = {
        val evict = totalSpansInMemStore >= maxEntries.get()
        if (evict) {
          evictionMeter.mark()
          totalSpansInMemStore -= eldest.getValue.builder.getChildSpansCount
          listeners.foreach(listener => listener.onEvict(eldest.getKey, eldest.getValue))
        }
        evict
      }
    }

    open = true

    LOGGER.info("Span buffer memory store has been initialized")
  }

  /**
    * get all buffered span objects that are recorded before the given timestamp
    *
    * @param forceEvictionTimestamp             forceEvictionTimestamp in millis for spanBuffers even if they are not complete
    * @param completedSpanBufferEvictionTimeout EvictionTimestamp in millis for completed spanBuffers
    * @return
    */
  def getAndRemoveSpanBuffersOlderThan(forceEvictionTimestamp: Long, completedSpanBufferEvictionTimeout: Long): (mutable.ListBuffer[SpanBufferWithMetadata], Option[OffsetAndMetadata]) = {
    val result = mutable.ListBuffer[SpanBufferWithMetadata]()
    var committableOffset = -1L

    val iterator = this.map.entrySet().iterator()
    var done = false

    while (!done && iterator.hasNext) {
      val el = iterator.next()
      if (el.getValue.firstSpanSeenAt <= completedSpanBufferEvictionTimeout) {
        if (el.getValue.firstSpanSeenAt <= forceEvictionTimestamp) {
          iterator.remove()
          totalSpansInMemStore -= el.getValue.builder.getChildSpansCount
          result += el.getValue
          if (committableOffset < el.getValue.firstSeenSpanKafkaOffset) committableOffset = el.getValue.firstSeenSpanKafkaOffset

        } else if (el.getValue.isrootSpanSeen) {
          iterator.remove()
          totalSpansInMemStore -= el.getValue.builder.getChildSpansCount
          result += el.getValue
        }
      } else {
        // here we apply a basic optimization and skip further iteration because all following records
        // in this map will have higher recordTimestamp. When we insert the first span for a unique traceId
        // in the map, we set the 'firstRecordTimestamp' attribute with record's timestamp
        done = true
      }
    }
    val commitableOffset = if (committableOffset >= 0) Some(new OffsetAndMetadata(committableOffset)) else None
    (result, commitableOffset)
  }

  override def addEvictionListener(l: EldestBufferedSpanEvictionListener): Unit = this.listeners += l

  override def close(): Unit = {
    if (open) {
      LOGGER.info("Closing the span buffer memory store")
      cacheSizer.removeCacheObserver(this)
      open = false
    }
  }

  def onCacheSizeChange(maxEntries: Int): Unit = {
    LOGGER.info("Cache size has been changed to " + maxEntries)
    this.maxEntries.set(maxEntries)
  }

  private def isRootSpan(span: Span): Boolean = {
    if (StringUtils.isEmpty(span.getParentSpanId)) {
      return true
    }
    false
  }

  override def addOrUpdateSpanBuffer(traceId: String, span: Span, spanRecordTimestamp: Long, offset: Long): SpanBufferWithMetadata = {
    val updatedValue = if (this.map.containsKey(traceId)) {

      val currentValue = this.map.get(traceId)
      val spanBuilder = currentValue.builder.addChildSpans(span)
      val rootSpanSeen = currentValue.isrootSpanSeen || isRootSpan(span)
      currentValue.copy(builder = spanBuilder, isrootSpanSeen = rootSpanSeen)
    }
    else {
      val spanBuffer = SpanBuffer.newBuilder().setTraceId(span.getTraceId).addChildSpans(span)
      SpanBufferWithMetadata(spanBuffer, spanRecordTimestamp, offset, isRootSpan(span))
    }
    totalSpansInMemStore += 1
    this.map.put(traceId, updatedValue)
    updatedValue
  }

  def totalSpans: Int = totalSpansInMemStore
}
