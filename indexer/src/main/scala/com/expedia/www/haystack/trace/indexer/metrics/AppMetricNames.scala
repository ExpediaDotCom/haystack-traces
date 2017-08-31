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

package com.expedia.www.haystack.trace.indexer.metrics

/**
  * list all app metric names that are published on jmx
  */
object AppMetricNames {
  val PUNCTUATE_TIMER = "buffer.punctuate"
  val PROCESS_TIMER = "buffer.process"
  val BUFFERED_SPANS_COUNT = "buffered.spans.count"
  val STATE_STORE_EVICTION = "state.store.eviction"
  val CHANGELOG_SEND_FAILURE = "changelog.send.failure"
  val SPAN_PROTO_DESER_FAILURE = "span.proto.deser.failure"
  val SPAN_BUFFER_PROTO_DESER_FAILURE = "span.buffer.proto.deser.failure"

  val CASSANDRA_WRITE_TIME = "cassandra.write.time"
  val CASSANDRA_WRITE_FAILURE = "cassandra.write.failure"
  val CASSANDRA_WRITE_WARNINGS = "cassandra.write.warnings"

  val ES_WRITE_FAILURE = "es.write.failure"
  val ES_WRITE_DUPLICATES = "es.writer.duplicates"
  val ES_WRITE_TIME = "es.writer.time"
}
