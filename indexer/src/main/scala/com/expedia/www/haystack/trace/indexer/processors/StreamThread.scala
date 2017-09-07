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

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trace.indexer.config.entities.KafkaConfiguration
import com.expedia.www.haystack.trace.indexer.processors.supplier.StreamProcessorSupplier
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class StreamThread(id: Int, kafkaConfig: KafkaConfiguration, processorSupplier: StreamProcessorSupplier[String, Span])
  extends Thread with AutoCloseable {

  private val LOGGER = LoggerFactory.getLogger(classOf[StreamThread])
  private val shutdownRequested = new AtomicBoolean(false)

  @volatile
  private var inRunningState = false

  @volatile
  private var rebalanceTriggered = false

  private val streamProcessors = new ConcurrentHashMap[TopicPartition, StreamProcessor[String, Span]]()

  private class RebalanceListener extends ConsumerRebalanceListener {
    override def onPartitionsRevoked(revokedPartitions: util.Collection[TopicPartition]): Unit = {
      LOGGER.info("Partitions {} revoked at the beginning of consumer rebalance", revokedPartitions)

      rebalanceTriggered = true

      revokedPartitions.foreach(
        p => {
          val processor = streamProcessors.remove(p)
          if (processor != null) processor.close()
        })
    }

    override def onPartitionsAssigned(assignedPartitions: util.Collection[TopicPartition]): Unit = {
      LOGGER.info("Partitions {} assigned at the beginning of consumer rebalance", assignedPartitions)

      rebalanceTriggered = true

      assignedPartitions foreach {
        partition => {
          val processor = processorSupplier.get()
          processor.init()
          streamProcessors.putIfAbsent(partition, processor)
        }
      }
    }
  }

  private var consumer = new KafkaConsumer[String, Span](kafkaConfig.consumerProps)
  private val rebalanceListener = new RebalanceListener

  consumer.subscribe(util.Arrays.asList(kafkaConfig.consumeTopic), rebalanceListener)

  /**
    * Execute the stream processors
    *
    * @throws Exception for any exceptions
    */
  override def run(): Unit = {
    LOGGER.info("Starting stream processing thread with id={}", id)
    try {
      inRunningState = true
      runLoop()
    } catch {
      case we: WakeupException =>
        // Ignore exception if shutdown has been requested
        if (!shutdownRequested.get()) throw we
      case e: Exception =>
        // may be logging the exception again for kafka specific exceptions, but it is ok.
        LOGGER.error("Stream application faced an exception during processing: ", e)
        throw e
    }
    finally {
      consumer.close(kafkaConfig.consumerCloseTimeoutInMillis, TimeUnit.MILLISECONDS)
      inRunningState = false
    }
  }

  private def waitForRebalanceToStabilize(): Unit = {
    while(rebalanceTriggered) {
      if(kafkaConfig.waitRebalanceTimeInMillis > 0) Thread.sleep(kafkaConfig.waitRebalanceTimeInMillis)
      rebalanceTriggered = false
    }
  }

  private def commit(offsets: util.HashMap[TopicPartition, OffsetAndMetadata], retryCount: Int = 0): Unit = {
    try {
      if(offsets.nonEmpty && retryCount <= kafkaConfig.commitOffsetRetries) consumer.commitSync(offsets)
    } catch {
      case _: CommitFailedException =>
        Thread.sleep(kafkaConfig.commitBackoffInMillis)
        // retry offset again
        commit(offsets, retryCount + 1)
      case ex: Exception =>
        LOGGER.error("Fail to commit the offsets with exception", ex)
    }
  }

  private def runLoop(): Unit = {
    while(!shutdownRequested.get()) {
      waitForRebalanceToStabilize()

      val records: ConsumerRecords[String, Span] = consumer.poll(kafkaConfig.pollTimeoutMs)
      if (records != null && !records.isEmpty && streamProcessors.nonEmpty) {

        val committableOffsets = new util.HashMap[TopicPartition, OffsetAndMetadata]()
        val groupedByPartition = records.groupBy(_.partition())

        groupedByPartition foreach {
          case (partition, partitionRecords) =>
            val topicPartition = new TopicPartition(kafkaConfig.consumeTopic, partition)
            val processor = streamProcessors.get(topicPartition)

            if(processor != null) {
              processor.process(partitionRecords) match {
                case Some(offsetMetadata) => committableOffsets.put(topicPartition, offsetMetadata)
                case _ => /* the processor has nothing to commit for now */
              }
            }
        }

        commit(committableOffsets)
      }
    }
  }

  override def close(): Unit = {
    shutdownRequested.set(true)
    consumer.wakeup()
  }

  def isStillRunning: Boolean = inRunningState
}
