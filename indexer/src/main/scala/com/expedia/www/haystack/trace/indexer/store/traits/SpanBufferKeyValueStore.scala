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

package com.expedia.www.haystack.trace.indexer.store.traits

import java.util

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trace.indexer.store.data.model.SpanBufferWithMetadata
import org.apache.kafka.streams.state.KeyValueStore

/**
  * this interface extends KeyValueStore to provide span buffering operations
  */
trait SpanBufferKeyValueStore extends KeyValueStore[String, SpanBufferWithMetadata] {

  /**
    * get all buffered span objects that are recorded before the given timestamp
    * @param timestamp timestamp in millis
    * @return
    */
  def getAndRemoveSpanBuffersOlderThan(timestamp: Long): util.Map[String, SpanBufferWithMetadata]

  /**
    * add a listener to the store, that gets called when the eldest spanBuffer is evicted
    * due to constraints of maxEntries in the store cache
    * @param l listener object that is called by the store
    */
  def addEvictionListener(l: EldestBufferedSpanEvictionListener): Unit

  /**
    * addd the spanBuffer if absent or update the spans for the given traceId
    * @param traceId traceId
    * @param span span object
    * @param spanRecordTimestamp timestamp of the span record
    */
  def addOrUpdateSpanBuffer(traceId: String, span: Span, spanRecordTimestamp: Long): SpanBufferWithMetadata
}
