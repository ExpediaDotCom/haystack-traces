package com.expedia.www.haystack.span.stitcher.integration

import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.{Properties, UUID}

import com.expedia.open.tracing.Span
import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.www.haystack.span.stitcher.integration.serdes.{SpanSerializer, StitchSpanDeserializer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.integration.utils.{EmbeddedKafkaCluster, IntegrationTestUtils}
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.{KeyValue, StreamsConfig}
import org.scalatest._

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

object EmbeddedKafka {
  val CLUSTER = new EmbeddedKafkaCluster(1)
  CLUSTER.start()
}

class BaseIntegrationTestSpec extends WordSpec with GivenWhenThen with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  case class TestSpanMetadata(traceId: String, spanIdPrefix: String)

  protected var scheduler: ScheduledExecutorService = null

  protected val PUNCTUATE_INTERVAL_MS = 2000
  protected val SPAN_STITCH_WINDOW_MS = 5000

  protected val PRODUCER_CONFIG = new Properties()
  protected val RESULT_CONSUMER_CONFIG = new Properties()
  protected val STREAMS_CONFIG = new Properties()
  protected val scheduledJobFuture: ScheduledFuture[_] = null

  protected var APP_ID = ""
  protected val INPUT_TOPIC = "spans"
  protected val OUTPUT_TOPIC = "stitchspans"

  override def beforeAll() {
    scheduler = Executors.newSingleThreadScheduledExecutor()
  }

  override def afterAll(): Unit = {
    scheduler.shutdownNow()
  }

  override def beforeEach() {
    EmbeddedKafka.CLUSTER.createTopic(INPUT_TOPIC)
    EmbeddedKafka.CLUSTER.createTopic(OUTPUT_TOPIC)

    PRODUCER_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EmbeddedKafka.CLUSTER.bootstrapServers)
    PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all")
    PRODUCER_CONFIG.put(ProducerConfig.RETRIES_CONFIG, "0")
    PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[SpanSerializer])

    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, EmbeddedKafka.CLUSTER.bootstrapServers)
    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, APP_ID + "-result-consumer")
    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StitchSpanDeserializer])

    STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, EmbeddedKafka.CLUSTER.bootstrapServers)
    STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-kstream")
    STREAMS_CONFIG.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[WallclockTimestampExtractor])
    STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
    STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "300")
    STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams")

    IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG)
  }

  override def afterEach(): Unit = {
    EmbeddedKafka.CLUSTER.deleteTopic(INPUT_TOPIC)
    EmbeddedKafka.CLUSTER.deleteTopic(OUTPUT_TOPIC)
  }

  def randomSpan(traceId: String,
                 spanId: String = UUID.randomUUID().toString,
                 startTime: Long = System.currentTimeMillis()): Span = {
    Span.newBuilder()
      .setTraceId(traceId)
      .setParentSpanId(UUID.randomUUID().toString)
      .setSpanId(spanId)
      .setOperationName("some-op")
      .setStartTime(startTime)
      .build()
  }

  protected def produceSpanAsync(maxSpans: Int, interval: FiniteDuration, toBeProducedSpans: List[TestSpanMetadata]): Unit = {
    var currentTime = System.currentTimeMillis()
    var idx = 0
    scheduler.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        if(idx < maxSpans) {
          currentTime = currentTime + ((idx * PUNCTUATE_INTERVAL_MS) / (maxSpans - 1))
          val records = toBeProducedSpans.map(rec => {
            new KeyValue[String, Span](rec.traceId, randomSpan(rec.traceId, s"${rec.spanIdPrefix}-$idx"))
          }).asJava
          IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            INPUT_TOPIC,
            records,
            PRODUCER_CONFIG,
            currentTime)
        }
        idx = idx + 1
      }
    }, 0, interval.toMillis, TimeUnit.MILLISECONDS)
  }

  protected def validateChildSpans(stitchedSpan: StitchedSpan, traceId: String, spanIdPrefix: String, childSpanCount: Int) = {
    stitchedSpan.getTraceId shouldBe traceId

    stitchedSpan.getChildSpansCount should (be >= 4 and be <= 5)

    (0 until stitchedSpan.getChildSpansCount).toList foreach { idx =>
      stitchedSpan.getChildSpans(idx).getSpanId shouldBe s"$spanIdPrefix-$idx"
      stitchedSpan.getChildSpans(idx).getTraceId shouldBe stitchedSpan.getTraceId
      stitchedSpan.getChildSpans(idx).getParentSpanId should not be null
    }
  }
}
