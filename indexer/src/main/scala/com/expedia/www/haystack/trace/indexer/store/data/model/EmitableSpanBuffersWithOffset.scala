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
 *     WITHOUT WARRANTIES OR CONDITIONS OF ASpanGroupWithTimestampNY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.expedia.www.haystack.trace.indexer.store.data.model


import org.apache.kafka.clients.consumer.OffsetAndMetadata

import scala.collection.mutable

/***
  * @param spanBuffers - list of span buffers which can be emitted
  * @param kafkaOffset - kafka offset which is safe to commit so that we don't loose any data
  */
case class EmitableSpanBuffersWithOffset(spanBuffers: mutable.ListBuffer[SpanBufferWithMetadata],kafkaOffset:Option[OffsetAndMetadata]) {

}
