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

package com.expedia.www.haystack.span.collector.writers

import com.expedia.open.tracing.buffer.SpanBuffer

/**
  * this class contains span buffer object and its serialized bytes. the consumers like
  * (see the [[com.expedia.www.haystack.span.collector.writers.SpanBufferWriter]] class)
  * have the option to use one or both without the need to serialize/deserialize
 *
  * @param spanBuffer           deserialized span buffer object
  * @param spanBufferProtoBytes serialized span buffer proto bytes
  */
case class SpanBufferRecord(spanBuffer: SpanBuffer, spanBufferProtoBytes: Array[Byte])
