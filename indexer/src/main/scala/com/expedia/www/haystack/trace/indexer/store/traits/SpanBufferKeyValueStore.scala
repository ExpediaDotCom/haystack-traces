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

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trace.indexer.store.data.model.{EmitableSpanBuffersWithOffset, SpanBufferWithMetadata}

/**
  * this interface extends KeyValueStore to provide span buffering operations
  */
trait SpanBufferKeyValueStore {


  /**
    * get all buffered span objects that are recorded before the given timestamp
    *
    * @param forceEvictionTimestamp             forceEvictionTimestamp in millis for spanBuffers even if they are not complete
    * @param completedSpanBufferEvictionTimeout EvictionTimestamp in millis for completed spanBuffers
    * @return
    */
  def getAndRemoveSpanBuffersOlderThan(forceEvictionTimestamp: Long, completedSpanBufferEvictionTimeout: Long): EmitableSpanBuffersWithOffset

  /**
    * add a listener to the store, that gets called when the eldest spanBuffer is evicted
    * due to constraints of maxEntries in the store cache
    *
    * @param listener listener object that is called by the store
    */
  def addEvictionListener(listener: EldestBufferedSpanEvictionListener): Unit

  /**
    * adds new spanBuffer for the traceId(if absent)in the store else add the spans
    *
    * @param traceId             traceId
    * @param span                span object
    * @param spanRecordTimestamp timestamp of the span record
    * @param offset              kafka offset of this span record
    */
  def addOrUpdateSpanBuffer(traceId: String, span: Span, spanRecordTimestamp: Long, offset: Long): SpanBufferWithMetadata

  /**
    * close the store
    */
  def close()

  /**
    * init the store
    */
  def init()
}
