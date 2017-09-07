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

package com.expedia.www.haystack.trace.indexer

import com.expedia.www.haystack.trace.indexer.config.entities._
import com.expedia.www.haystack.trace.indexer.processors.StreamThread
import com.expedia.www.haystack.trace.indexer.processors.supplier.SpanIndexProcessorSupplier
import com.expedia.www.haystack.trace.indexer.store.SpanBufferMemoryStoreSupplier
import com.expedia.www.haystack.trace.indexer.writers.TraceWriter
import com.expedia.www.haystack.trace.indexer.writers.cassandra.CassandraWriter
import com.expedia.www.haystack.trace.indexer.writers.es.ElasticSearchWriter
import com.expedia.www.haystack.trace.indexer.writers.kafka.KafkaWriter
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable

class StreamRunner(kafkaConfig: KafkaConfiguration,
                   accumulatorConfig: SpanAccumulatorConfiguration,
                   esConfig: ElasticSearchConfiguration,
                   cassandraConfig: CassandraConfiguration,
                   indexConfig: IndexConfiguration) extends AutoCloseable {

  implicit private val executor = scala.concurrent.ExecutionContext.Implicits.global

  private val LOGGER = LoggerFactory.getLogger(classOf[StreamRunner])

  private val writers: Seq[TraceWriter] = {
    val writers = mutable.ListBuffer[TraceWriter]()
    writers += new CassandraWriter(cassandraConfig)(executor)
    writers += new ElasticSearchWriter(esConfig, indexConfig)

    if(StringUtils.isNotEmpty(kafkaConfig.produceTopic)) {
      writers += new KafkaWriter(kafkaConfig.producerProps, kafkaConfig.produceTopic)
    }
    writers
  }

  private var threads: mutable.ListBuffer[StreamThread] = _

  def start(): Unit = {
    LOGGER.info("Starting the span indexing stream..")

    threads = mutable.ListBuffer[StreamThread]()
    val storeSupplier = new SpanBufferMemoryStoreSupplier(accumulatorConfig.minTracesPerCache, accumulatorConfig.maxEntriesAllStores)
    val streamProcessSupplier = new SpanIndexProcessorSupplier(accumulatorConfig, storeSupplier, writers)

    (0 until kafkaConfig.numStreamThreads).toList foreach {
      streamId => {
        val thread = new StreamThread(streamId, kafkaConfig, streamProcessSupplier)
        threads += thread
        thread.start()
      }
    }
  }

  private[haystack] def closeStreamThreads(): Unit = {
    threads.foreach (_.close())
  }

  private def closeWriters(): Unit = {
    writers foreach {
      writer => writer.close()
    }
  }

  override def close(): Unit = {
    LOGGER.info("Closing the span indexing threads, and cassandra and elastic search clients.")
    closeStreamThreads()
    closeWriters()

    LOGGER.info("Will wait for still running stream threads before they die.")
    for (thread <- threads) {
      try {
        if (thread.isStillRunning) thread.join()
      } catch {
        case _: InterruptedException =>
          LOGGER.error("Close thread has been interrupted, stream threads are still running and wait has timed out!!")
      }
    }
  }
}
