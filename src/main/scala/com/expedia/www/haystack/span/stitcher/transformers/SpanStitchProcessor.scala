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
package com.expedia.www.haystack.span.stitcher.transformers

import com.expedia.www.haystack.span.stitcher.config.KafkaStreamsConfiguration
import org.apache.kafka.streams.processor.{Processor, ProcessorContext}
import com.expedia.open.tracing.Span
import com.expedia.www.haystack.span.stitcher.metrics.MetricsSupport

class SpanStitchProcessor(config: KafkaStreamsConfiguration) extends Processor[Array[Byte], Span] with MetricsSupport {

  override def init(context: ProcessorContext): Unit = {
    context.getStateStore("stitched-span-store")
  }

  override def punctuate(timestamp: Long): Unit = ???

  override def process(key: Array[Byte], value: Span): Unit = ???

  override def close(): Unit = ???
}
