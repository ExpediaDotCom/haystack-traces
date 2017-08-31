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

package com.expedia.www.haystack.trace.indexer.processors

import com.expedia.open.tracing.buffer.SpanBuffer
import com.expedia.www.haystack.trace.indexer.writers.cassandra.CassandraWriter
import com.expedia.www.haystack.trace.indexer.writers.es.ElasticSearchWriter
import org.apache.kafka.streams.processor.{Processor, ProcessorContext}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

class TraceWriteProcessor(cassandraWriter: CassandraWriter,
                          elasticSearchWriter: ElasticSearchWriter)(implicit val dispatcher: ExecutionContextExecutor)
  extends Processor[String, SpanBuffer] {

  // TODO: put this as config
  private val MAX_WRITE_WAIT = 10.seconds
  private var context: ProcessorContext = _

  /**
    * initializes the span buffering processor
    *
    * @param context processor context object
    */
  override def init(context: ProcessorContext): Unit = {
    this.context = context
  }

  /**
    * writes the span buffer of trace to external store
    * @param traceId  the partition key for spans will always be its traceId
    * @param spanBuffer: span buffer
    */
  override def process(traceId: String, spanBuffer: SpanBuffer): Unit = {
    val spanBufferBytes = spanBuffer.toByteArray

    val writeFuture = Future.sequence(Seq(
      cassandraWriter.write(traceId, spanBufferBytes),
      elasticSearchWriter.write(traceId, spanBuffer)
    ))

    Await.ready(writeFuture, MAX_WRITE_WAIT)
    this.context.forward(traceId, spanBufferBytes)
  }

  /**
    * nothing to do in punctuate
    * @param timestamp: timestamp of the record
    */
  override def punctuate(timestamp: Long): Unit = ()

  /**
    * close the processor
    */
  override def close(): Unit = ()
}
