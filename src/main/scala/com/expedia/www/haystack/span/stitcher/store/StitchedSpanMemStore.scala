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
package com.expedia.www.haystack.span.stitcher.store

import com.expedia.www.haystack.span.stitcher.store.data.model.StitchedSpanWithMetadata
import com.expedia.www.haystack.span.stitcher.store.traits.StitchedSpanKVStore
import org.apache.kafka.streams.state.KeyValueIterator

abstract class StitchedSpanMemStore extends StitchedSpanKVStore {
  @volatile protected var open = false

  override def flush(): Unit = ()

  override def persistent(): Boolean = false

  override def close(): Unit = open = false

  override def isOpen: Boolean = open

  override def range(from: Array[Byte], to: Array[Byte]): KeyValueIterator[Array[Byte], StitchedSpanWithMetadata] = {
    throw new UnsupportedOperationException("StitchedSpanMemStore does not support range() function.")
  }

  override def all(): KeyValueIterator[Array[Byte], StitchedSpanWithMetadata] = {
    throw new UnsupportedOperationException("StitchedSpanMemStore does not support all() function.")
  }
}
