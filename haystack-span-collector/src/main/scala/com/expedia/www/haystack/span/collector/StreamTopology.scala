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

package com.expedia.www.haystack.span.collector

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{CommittableOffsetBatch, _}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{ActorMaterializer, KillSwitch, KillSwitches}
import com.expedia.www.haystack.span.collector.config.entities.CollectorConfiguration
import com.expedia.www.haystack.span.collector.serdes.SpanBufferDeserializer
import com.expedia.www.haystack.span.collector.writers.{SpanBufferWriter, SpanBufferRecord}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

class StreamTopology(collectorConfig: CollectorConfiguration,
                     writers: Seq[SpanBufferWriter])(implicit val system: ActorSystem) {

  implicit val materializer = ActorMaterializer()
  implicit val dispatcher = system.dispatcher
  private val spanBufferDeser = new SpanBufferDeserializer()

  /**
    * build and start the topology.
    * The topology reads span buffer proto objects from kafka, groups them, writes to cassandra and elastic search together
    * and commits the offset to kafka. The kafka offsets are also grouped before they are committed.
    */
  def start(): (KillSwitch, Future[Done]) = {
    val settings = ConsumerSettings.create(system, new ByteArrayDeserializer, new ByteArrayDeserializer)

    Consumer
      .committableSource(settings, Subscriptions.topics(collectorConfig.consumerTopic))
      .viaMat(KillSwitches.single)(Keep.right)
      .groupedWithin(collectorConfig.batchSize, collectorConfig.batchTimeoutMillis.millis)
      .mapAsync(collectorConfig.parallelism) { writeSpans }
      .mapConcat(toImmutable)
      .batch(collectorConfig.commitBatch, first => CommittableOffsetBatch.empty.updated(first)) {
        (batch, elem) => batch.updated(elem)
      }
      .mapAsync(collectorConfig.parallelism)(_.commitScaladsl())
      .toMat(Sink.ignore)(Keep.both)
      .run()
  }

  /**
    * writes the span records to elastic search and cassandra asynchronously
    * @param records set of span buffer records
    * @return a future that contains the completion or failure for both the writers. The future result contains the
    *         offset of the read records.
    */
  private def writeSpans(records: Seq[CommittableMessage[Array[Byte], Array[Byte]]]): Future[Seq[CommittableOffset]] = {
    val promise = Promise[Seq[CommittableMessage[Array[Byte], Array[Byte]]]]()

    val spanBuffers: Seq[SpanBufferRecord] = records
      .map(rec => SpanBufferRecord(spanBufferDeser.deserialize(rec.record.value()), rec.record.value()))
      .filter(el => el.spanBuffer != null)

    val allWrites: Seq[Future[Any]] = writers.map(_.write(spanBuffers))
    Future.sequence(allWrites).onComplete(_ => promise.success(records))
    promise.future.map(writeResult => writeResult.map(_.committableOffset))
  }

  private def toImmutable[A](elements: Iterable[A]) = {
    new scala.collection.immutable.Iterable[A] {
      override def iterator: Iterator[A] = elements.toIterator
    }
  }
}
