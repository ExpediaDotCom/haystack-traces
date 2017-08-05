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

import com.expedia.open.tracing.stitch.StitchedSpan
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.internals.{ProcessorStateManager, RecordCollector}
import org.apache.kafka.streams.state.StateSerdes

/**
  * unfortunately the change logger class in KStreams package is marked internal and can not be used
  * the idea behind this change logger is to push the state changes to changelog topic
  * @param name: name of the store
  * @param context: processor context
  * @param serialization: serde used to serialize the key and values
  */
class StitchedSpanStoreChangeLogger(val name: String,
                                    val context: ProcessorContext,
                                    val serialization: StateSerdes[String, StitchedSpan]) {
  private val topic = ProcessorStateManager.storeChangelogTopic(context.applicationId, name)
  private val collector = context.asInstanceOf[RecordCollector.Supplier].recordCollector
  private val partition = context.taskId().partition

  def logChange(key: String, value: StitchedSpan): Unit = {
    if (collector != null) {
      val keySerializer = serialization.keySerializer
      val valueSerializer = serialization.valueSerializer
      collector.send(this.topic, key, value, this.partition, context.timestamp, keySerializer, valueSerializer)
    }
  }
}
