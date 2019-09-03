package com.expedia.www.haystack.trace.indexer.unit

import java.util.concurrent.TimeUnit

import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.trace.commons.clients.es.document.ShowValuesDoc
import com.expedia.www.haystack.trace.commons.config.entities.{IndexFieldType, WhiteListIndexFields, WhitelistIndexField, WhitelistIndexFieldConfiguration}
import com.expedia.www.haystack.trace.indexer.config.ProjectConfiguration
import com.expedia.www.haystack.trace.indexer.writers.es.ShowValuesDocumentGenerator
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.{FunSpec, Matchers}

import scala.collection.mutable

class ShowValuesDocumentGeneratorSpec extends FunSpec with Matchers {
  protected implicit val formats: Formats = DefaultFormats + new EnumNameSerializer(IndexFieldType)

  private val TRACE_ID = "trace_id"
  private val START_TIME_1 = 1529042838469123l
  private val START_TIME_2 = 1529042848469000l
  private val project = new ProjectConfiguration

  private val LONG_DURATION = TimeUnit.SECONDS.toMicros(25) + TimeUnit.MICROSECONDS.toMicros(500)

  describe("Show values document generator") {
    it("should create show value docs for given servicename and fieldvaluesmap") {
      //Whatever is passed through Whitelistindexconfiguration is irrelevant to the generator for this test
      val generator = new ShowValuesDocumentGenerator(project.showValuesConfig, WhitelistIndexFieldConfiguration())
      val serviceName = "service1"
      val fieldValuesMap: mutable.HashMap[String, mutable.Set[String]] = new mutable.HashMap[String, mutable.Set[String]]();
      fieldValuesMap.update("page-name", mutable.Set[String]("pageA", "pageB"))
      fieldValuesMap.update("page-id", mutable.Set[String]("1", "2"))

      val docs = generator.createShowValuesDoc(serviceName, fieldValuesMap)
      docs.head shouldEqual ShowValuesDoc(serviceName, "page-id", "2")
      docs(1) shouldEqual ShowValuesDoc(serviceName, "page-id", "1")
      docs(2) shouldEqual ShowValuesDoc(serviceName, "page-name", "pageA")
      docs(3) shouldEqual ShowValuesDoc(serviceName, "page-name", "pageB")
      docs.size shouldEqual 4
    }

    it("should create showvaluedocuments for given spans using the whitelistfieldconfiguration(empty) and return showValues docs accumulated if conditions met") {
      val config = WhitelistIndexFieldConfiguration()
      val generator = new ShowValuesDocumentGenerator(project.showValuesConfig, config)

      val span_1 = Span.newBuilder().setTraceId(TRACE_ID)
        .setSpanId("span-1")
        .setServiceName("service1")
        .setOperationName("op1")
        .setStartTime(START_TIME_1)
        .setDuration(610000L)
        .addTags(Tag.newBuilder().setKey("page-name").setVStr("pageA"))
        .addTags(Tag.newBuilder().setKey("page-id").setVStr("1"))
        .build()
      val span_2 = Span.newBuilder().setTraceId(TRACE_ID)
        .setSpanId("span-2")
        .setServiceName("service2")
        .setOperationName("op1")
        .setStartTime(START_TIME_1)
        .setDuration(500000L)
        .addTags(Tag.newBuilder().setKey("page-name").setVStr("pageB"))
        .addTags(Tag.newBuilder().setKey("page-id").setVStr("2"))
        .build()
      val span_3 = Span.newBuilder().setTraceId(TRACE_ID)
        .setSpanId("span-3")
        .setServiceName("service1")
        .setDuration(LONG_DURATION)
        .setStartTime(START_TIME_2)
        .addTags(Tag.newBuilder().setKey("page-name").setVStr("pageB"))
        .addTags(Tag.newBuilder().setKey("page-id").setVStr("2"))
        .setOperationName("op3").build()

      val docs1 = generator.getAndUpdateShowValues(List(span_1, span_2, span_3))
      //Because whitelistfields are empty
      docs1.size shouldEqual 0

      val whitelistField_1 = WhitelistIndexField(name = "page-name", `type` = IndexFieldType.string, showValue = true)
      val whitelistField_2 = WhitelistIndexField(name = "page-id", `type` = IndexFieldType.string, showValue = true)

      val cfgJsonData = Serialization.write(WhiteListIndexFields(List(whitelistField_1, whitelistField_2)))

      // reload with the given whitelisted fields
      config.onReload(cfgJsonData)
      val docs2 = generator.getAndUpdateShowValues(List(span_1, span_2, span_3))
      docs2.size shouldEqual 6
      docs2.map(d => d.servicename) should contain allOf ("service1", "service2")
      docs2.map(d => d.fieldname) should contain allOf ("page-name", "page-id")
      docs2.map(d => d.fieldvalue) should contain allOf ("pageA", "pageB", "1", "2")
      docs2.foreach(d => List)
    }
  }
}
