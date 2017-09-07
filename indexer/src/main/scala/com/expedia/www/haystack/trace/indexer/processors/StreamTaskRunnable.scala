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
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trace.indexer.config.entities.KafkaConfiguration
import com.expedia.www.haystack.trace.indexer.processors.StreamTaskState.StreamTaskState
import com.expedia.www.haystack.trace.indexer.processors.supplier.StreamProcessorSupplier
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

class StreamTaskRunnable(taskId: Int, kafkaConfig: KafkaConfiguration, processorSupplier: StreamProcessorSupplier[String, Span])
  extends Runnable with AutoCloseable {

  private val LOGGER = LoggerFactory.getLogger(classOf[StreamTaskRunnable])

  private class RebalanceListener extends ConsumerRebalanceListener {
    override def onPartitionsRevoked(revokedPartitions: util.Collection[TopicPartition]): Unit = {
      LOGGER.info("Partitions {} revoked at the beginning of consumer rebalance for taskId={}", revokedPartitions, taskId)

      revokedPartitions.foreach(
        p => {
          val processor = streamProcessors.remove(p)
          if (processor != null) processor.close()
        })
    }

    override def onPartitionsAssigned(assignedPartitions: util.Collection[TopicPartition]): Unit = {
      LOGGER.info("Partitions {} assigned at the beginning of consumer rebalance for taskId={}", assignedPartitions, taskId)

      assignedPartitions foreach {
        partition => {
          val processor = processorSupplier.get()
          processor.init()
          streamProcessors.putIfAbsent(partition, processor)
        }
      }
    }
  }

  @volatile
  private var state = StreamTaskState.NOT_RUNNING

  private val shutdownRequested = new AtomicBoolean(false)
  private val wakeupScheduler = Executors.newScheduledThreadPool(1)
  private var wakeups: Int = 0
  private val listeners = mutable.ListBuffer[StateListener]()

  private val streamProcessors = new ConcurrentHashMap[TopicPartition, StreamProcessor[String, Span]]()
  private var consumer = new KafkaConsumer[String, Span](kafkaConfig.consumerProps)
  private val rebalanceListener = new RebalanceListener

  consumer.subscribe(util.Arrays.asList(kafkaConfig.consumeTopic), rebalanceListener)

  /**
    * Execute the stream processors
    *
    * @throws Exception for any exceptions
    */
  override def run(): Unit = {
    LOGGER.info("Starting stream processing thread with id={}", taskId)
    try {
      updateStateChangeAndNotify(StreamTaskState.RUNNING)
      runLoop()
    } catch {
      case ie: InterruptedException =>
        LOGGER.error(s"This stream task has been interrupted for taskId=$taskId", ie)
      case ex: Exception =>
        if(!shutdownRequested.get()) updateStateChangeAndNotify(StreamTaskState.FAILED)
        // may be logging the exception again for kafka specific exceptions, but it is ok.
        LOGGER.error(s"Stream application faced an exception during processing for taskId=$taskId: ", ex)
    }
    finally {
      consumer.close(kafkaConfig.consumerCloseTimeoutInMillis, TimeUnit.MILLISECONDS)
      updateStateChangeAndNotify(StreamTaskState.NOT_RUNNING)
    }
  }

  private def runLoop(): Unit = {

    while(!shutdownRequested.get()) {
      poll() match {
        case Some(records) if records != null && !records.isEmpty && streamProcessors.nonEmpty =>
          val committableOffsets = new util.HashMap[TopicPartition, OffsetAndMetadata]()
          val groupedByPartition = records.groupBy(_.partition())

          groupedByPartition foreach {
            case (partition, partitionRecords) =>
              val topicPartition = new TopicPartition(kafkaConfig.consumeTopic, partition)
              val processor = streamProcessors.get(topicPartition)

              if (processor != null) {
                processor.process(partitionRecords) match {
                  case Some(offsetMetadata) => committableOffsets.put(topicPartition, offsetMetadata)
                  case _ => /* the processor has nothing to commit for now */
                }
              }
          }
          commit(committableOffsets)
        // if no records are returned in poll, then do nothing
        case _ =>
      }
    }
  }

  private def poll(): Option[ConsumerRecords[String, Span]] = {

    def handleWakeup(we: WakeupException): Unit = {
      if(!shutdownRequested.get()) {
        wakeups = wakeups + 1
        if (wakeups == kafkaConfig.maxWakeups) {
          LOGGER.error(s"WakeupException limit exceeded, throwing up wakeup exception for taskId=$taskId.", we)
          throw we
        } else {
          LOGGER.error(s"Consumer poll took more than ${kafkaConfig.wakeupTimeoutInMillis} ms for taskId=$taskId, wakeup attempt=$wakeups!. Will try poll again!")
        }
      } else throw we
    }

    val wakeupCall = wakeupScheduler.schedule(new Runnable {
      override def run(): Unit = consumer.wakeup()
    }, kafkaConfig.wakeupTimeoutInMillis, TimeUnit.MILLISECONDS)

    try {
      val records: ConsumerRecords[String, Span] = consumer.poll(kafkaConfig.pollTimeoutMs)
      wakeups = 0
      Some(records)
    } catch {
      case we: WakeupException =>
        handleWakeup(we)
        None
    } finally {
      Try(wakeupCall.cancel(true))
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

  private def updateStateChangeAndNotify(newState: StreamTaskState) = {
    if(state != newState) {
      state = newState
      listeners foreach (listener => listener.onChange(state))
    }
  }

  override def close(): Unit = {
    Try {
      LOGGER.info(s"Close has been requested for taskId=$taskId")
      shutdownRequested.set(true)
      if (isStillRunning) consumer.wakeup()
      wakeupScheduler.shutdown()
    }
  }

  def isStillRunning: Boolean = state == StreamTaskState.RUNNING

  def setStateListener(listener: StateListener): Unit = listeners += listener
}
