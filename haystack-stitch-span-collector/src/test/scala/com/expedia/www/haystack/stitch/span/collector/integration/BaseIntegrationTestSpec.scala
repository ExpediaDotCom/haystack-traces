package com.expedia.www.haystack.stitch.span.collector.integration

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.datastax.driver.core.{Cluster, Session, SimpleStatement}
import com.expedia.open.tracing.Tag.TagType
import com.expedia.open.tracing.stitch.StitchedSpan
import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.stitch.span.collector.config.entities.{IndexConfiguration, IndexField}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Index, Search}
import io.searchbox.indices.mapping.PutMapping
import io.searchbox.indices.{CreateIndex, DeleteIndex}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.scalatest._

import scala.collection.JavaConversions._

abstract class BaseIntegrationTestSpec extends WordSpec with GivenWhenThen with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  case class CassandraRow(id: String, timestamp: java.util.Date, stitchedSpan: StitchedSpan)

  implicit val formats = DefaultFormats

  private val KAFKA_BROKERS = "kafkasvc:9092"
  protected val CONSUMER_TOPIC = "stitch-spans"
  private val CASSANDRA_ENDPOINT = "cassandra"
  private val CASSANDRA_KEYSPACE = "haystack"

  private val ELASTIC_SEARCH_ENDPOINT = "http://elasticsearch:9200"
  private val SPANS_INDEX_TYPE = "spans"
  private val HAYSTACK_SPAN_INDEX = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    s"haystack-span-${formatter.format(new Date())}"
  }

  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _
  private var esClient: JestClient = _
  private var cassandraSession: Session = _

  private def createIndexConfigInES(): Unit = {
    val request = new Index.Builder(indexConfigInDatabase()).index("reload-configs").`type`("indexing-fields").build()
    val result = esClient.execute(request)
    if (!result.isSucceeded) {
      fail(s"Fail to create the configuration in ES for 'indexing' fields with message '${result.getErrorMessage}'")
    }
  }

  private def dropIndexes(): Unit = {
    esClient.execute(new DeleteIndex.Builder(HAYSTACK_SPAN_INDEX).build())
    esClient.execute(new DeleteIndex.Builder("reload-configs").build())
  }

  private def deleteCassandraTableRows(): Unit = {
    cassandraSession.execute(new SimpleStatement("TRUNCATE stitchedspans"))
  }

  private def createAndUpdaeteIndexMappings(): Unit = {
    val createIndexRequest = new CreateIndex.Builder(HAYSTACK_SPAN_INDEX).build()
    esClient.execute(createIndexRequest)
    val putMappingSource = """
                                |{
                                |    "spans": {
                                |      "properties": {
                                |        "spans": {
                                |          "type": "nested"
                                |        }
                                |      }
                                |   }
                                |}
                              """.stripMargin
    val mappings = new PutMapping.Builder(HAYSTACK_SPAN_INDEX, SPANS_INDEX_TYPE, putMappingSource).build()
    val res = esClient.execute(mappings)
    if(!res.isSucceeded) {
      fail("Fail to update the mappings for elastic search index")
    }
  }

  override def beforeAll() {
    producer = {
      val properties = new Properties()
      properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS)
      properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getCanonicalName)
      properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getCanonicalName)
      new KafkaProducer[Array[Byte], Array[Byte]](properties)
    }

    esClient = {
      val factory = new JestClientFactory()
      factory.setHttpClientConfig(new HttpClientConfig.Builder(ELASTIC_SEARCH_ENDPOINT).build())
      factory.getObject
    }

    cassandraSession = Cluster.builder().addContactPoints(CASSANDRA_ENDPOINT).build().connect(CASSANDRA_KEYSPACE)

    deleteCassandraTableRows()

    // drop the haystack-span index
    dropIndexes()

    // add indexable tags as a config in ES
    createIndexConfigInES()

    // createIndex

    // setup the mappings for index that stores all the spans
    createAndUpdaeteIndexMappings()

    // wait for few seconds(5 sec is the schedule interval) to let app consume the new indexing config
    Thread.sleep(6000)
  }

  override def afterAll(): Unit = {
    if (producer != null) producer.close()
    if (esClient != null) esClient.shutdownClient()
    if (cassandraSession != null) cassandraSession.close()
  }

  protected def produceToKafka(stitchedSpans: Seq[StitchedSpan]): Unit = {
    stitchedSpans foreach { st =>
      val record = new ProducerRecord[Array[Byte], Array[Byte]](CONSUMER_TOPIC, st.getTraceId.getBytes, st.toByteArray)
      producer.send(record, new Callback() {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception != null) {
            fail("Fail to produce the stitched span to kafka with error", exception)
          }
        }
      })
    }
    producer.flush()
  }

  protected def queryAllCassandra(): Seq[CassandraRow] = {
    val rows = cassandraSession.execute("SELECT id, ts, stitchedspans from stitchedspans")
    rows.map(row => {
      CassandraRow(row.getString("id"), row.getTimestamp("ts"), StitchedSpan.parseFrom(row.getBytes("stitchedspans")))
    }).toList
  }

  protected def queryElasticSearch(query: String): List[String] = {
    val searchQuery = new Search.Builder(query)
      .addIndex(HAYSTACK_SPAN_INDEX)
      .addType(SPANS_INDEX_TYPE)
      .build()
    val result = esClient.execute(searchQuery)
    if (result.getSourceAsStringList == null) Nil else result.getSourceAsStringList.toList
  }

  protected def createStitchedSpans(total: Int, spanCount: Int, duration: Long): Seq[StitchedSpan] = {
    (0 until total).toList map { traceId =>
      val stitchedSpanBuilder = StitchedSpan.newBuilder()
      stitchedSpanBuilder.setTraceId(traceId.toString)

      // add spans
      (0 until spanCount).toList foreach { spanId =>
        val process = com.expedia.open.tracing.Process.newBuilder().setServiceName(s"service$spanId")
        val span = Span.newBuilder()
          .setTraceId(traceId.toString)
          .setProcess(process)
          .setOperationName(s"op$spanId")
          .setDuration(duration)
          .setSpanId(s"${traceId.toString}_${spanId.toString}")
          .addTags(Tag.newBuilder().setKey("errorcode").setType(TagType.LONG).setVLong(404))
          .addTags(Tag.newBuilder().setKey("role").setType(TagType.STRING).setVStr("haystack"))
          .build()
        stitchedSpanBuilder.addChildSpans(span)
      }
      stitchedSpanBuilder.build()
    }
  }

  private def indexConfigInDatabase(): String = {
    val indexTagFields = List(
      IndexField(name = "role", `type` = "string", enabled = true),
      IndexField(name = "errorcode", `type` = "long", enabled = true))
    Serialization.write(IndexConfiguration(indexTagFields))
  }
}