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

package com.expedia.www.haystack.trace.indexer.processors.supplier

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.indexer.processors.TraceWriteProcessor
import com.expedia.www.haystack.trace.indexer.writers.cassandra.CassandraWriter
import com.expedia.www.haystack.trace.indexer.writers.es.ElasticSearchWriter
import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier}

import scala.concurrent.ExecutionContextExecutor

class TraceWriteProcessorSupplier(cassandraWriter: CassandraWriter,
                                  elasticSearchWriter: ElasticSearchWriter)(implicit val dispatcher: ExecutionContextExecutor)
  extends ProcessorSupplier[String, SpanBuffer] {

  /**
    * @return processor that writes span buffer to external stores like cassandra and es
    */
  override def get(): Processor[String, SpanBuffer] = new TraceWriteProcessor(cassandraWriter, elasticSearchWriter)
}
